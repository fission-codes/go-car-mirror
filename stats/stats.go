package stats

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/util"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("go-car-mirror")

// Stats is an interface for recording events.
type Stats interface {
	// Log records an event.
	Log(string)
	// LogInterval records an interval in nanoseconds.
	LogInterval(string, time.Duration)
	// LogBytes records an amount in bytes.
	LogBytes(string, uint64)
	// WithContext returns a new Stats instance that adds a prefix to all events.
	WithContext(string) Stats
	// Logger returns a logger that adds a prefix to all events.
	Logger() *zap.SugaredLogger
	// Name returns the name of this Stats instance.
	Name() string
}

// Context is a Stats implementation that adds a prefix to all events.
type Context struct {
	parent Stats
	name   string
	logger *zap.SugaredLogger
}

// Log records an event.
func (ctx *Context) Log(event string) {
	ctx.parent.Log(ctx.name + "." + event)
}

// Log records an event.
func (ctx *Context) LogInterval(event string, interval time.Duration) {
	ctx.parent.LogInterval(ctx.name+"."+event, interval)
}

func (ctx *Context) LogBytes(event string, bytes uint64) {
	ctx.parent.LogBytes(ctx.name+"."+event, bytes)
}

// Logger returns a logger that adds a prefix to all events.
func (ctx *Context) Logger() *zap.SugaredLogger {
	return ctx.logger
}

// Name returns the name of this Stats instance.
func (ctx *Context) Name() string {
	return ctx.parent.Name() + ctx.name
}

// WithContext returns a new Stats instance that adds a prefix to all events.
func (ctx *Context) WithContext(name string) Stats {
	return &Context{
		parent: ctx,
		name:   name,
		logger: ctx.logger.With("for", name),
	}
}

type Bucket struct {
	Count    uint64
	Bytes    uint64
	Interval time.Duration
}

func Diff(a, b *Bucket) *Bucket {
	if b == nil && a == nil {
		return &Bucket{}
	}
	if b == nil {
		return a
	}
	if a == nil {
		return b
	}
	return &Bucket{
		Count:    util.Diff(b.Count, a.Count),
		Bytes:    util.Diff(b.Bytes, a.Bytes),
		Interval: util.Diff(b.Interval, a.Interval),
	}
}

// Snapshot is a snapshot of a Stats instance.
type Snapshot struct {
	values map[string]*Bucket
}

// Count returns the number of times the given event has been recorded.
func (snap *Snapshot) Count(event string) uint64 {
	return snap.values[event].Count
}

func (snap *Snapshot) Bytes(event string) uint64 {
	return snap.values[event].Bytes
}

func (snap *Snapshot) Interval(event string) time.Duration {
	return snap.values[event].Interval
}

// Keys returns a sorted list of all events that have been recorded.
func (snap *Snapshot) Keys() []string {
	return maps.Keys(snap.values)
}

// Diff returns a new Snapshot that contains the difference between this Snapshot and the given Snapshot.
func (snap *Snapshot) Diff(other *Snapshot) *Snapshot {
	result := make(map[string]*Bucket)
	for key, value := range snap.values {
		result[key] = value
	}
	for key, otherValue := range other.values {
		result[key] = Diff(result[key], otherValue)
	}
	return &Snapshot{values: result}
}

// Filter returns a new Snapshot that contains only the events that match the given prefix.
func (snap *Snapshot) Filter(prefix string) *Snapshot {
	result := make(map[string]*Bucket)
	for key, value := range snap.values {
		if strings.HasPrefix(key, prefix) {
			result[key] = value
		}
	}
	return &Snapshot{values: result}
}

// Write writes the snapshot to the given logger.
func (snap *Snapshot) Write(log *zap.SugaredLogger) {
	keys := maps.Keys(snap.values)
	sort.Strings(keys)
	for _, key := range keys {
		bucket := snap.values[key]
		if bucket.Count > 0 {
			log.Infow("snapshot", "event", key, "count", bucket.Count)
		}
		if bucket.Bytes > 0 {
			log.Infow("snapshot", "event", key, "bytes", bucket.Bytes)
		}
		if bucket.Interval > 0 {
			log.Infow("snapshot", "event", key, "interval", bucket.Interval)
		}
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (snap *Snapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(snap.values)
}

// Reporting is an interface for reporting stats.
type Reporting interface {
	Count(string) uint64
	Snapshot() *Snapshot
}

// DefaultStatsAndReporting is a Stats and Reporting implementation that records events in memory.
type DefaultStatsAndReporting struct {
	mutex  sync.RWMutex
	values map[string]*Bucket
	logger *zap.SugaredLogger
}

// NewDefaultStatsAndReporting returns a new DefaultStatsAndReporting instance.
func NewDefaultStatsAndReporting() *DefaultStatsAndReporting {
	return &DefaultStatsAndReporting{
		mutex:  sync.RWMutex{},
		values: make(map[string]*Bucket),
		logger: &log.SugaredLogger,
	}
}

func (ds *DefaultStatsAndReporting) getOrInitBucket(event string) *Bucket {
	// Don't make this public - it doesn't have a mutex
	val, ok := ds.values[event]
	// If the key exists
	if !ok {
		val = &Bucket{}
		ds.values[event] = val
	}
	return val
}

// Log records an event.
func (ds *DefaultStatsAndReporting) Log(event string) {
	ds.mutex.Lock()
	ds.getOrInitBucket(event).Count++
	ds.mutex.Unlock()
}

func (ds *DefaultStatsAndReporting) LogBytes(event string, bytes uint64) {
	ds.mutex.Lock()
	ds.getOrInitBucket(event).Bytes += bytes
	ds.mutex.Unlock()
}

func (ds *DefaultStatsAndReporting) LogInterval(event string, interval time.Duration) {
	ds.mutex.Lock()
	ds.getOrInitBucket(event).Interval += interval
	ds.mutex.Unlock()
}

// WithContext returns a new Context instance that adds a prefix to all events.
func (ds *DefaultStatsAndReporting) WithContext(name string) Stats {
	return &Context{
		parent: ds,
		name:   name,
		logger: ds.logger,
	}
}

// Logger returns a logger that adds a prefix to all events.
func (ds *DefaultStatsAndReporting) Logger() *zap.SugaredLogger {
	return ds.logger.With("for", ds.Name())
}

// Name returns the name of this DefaultStatsAndReporting instance.
func (ds *DefaultStatsAndReporting) Name() string {
	return "root"
}

// Count returns the number of times the given event has been recorded.
func (ds *DefaultStatsAndReporting) Count(event string) uint64 {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	return ds.getOrInitBucket(event).Count
}

// Snapshot returns a snapshot of the current DefaultStatsAndReporting instance.
func (ds *DefaultStatsAndReporting) Snapshot() *Snapshot {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	values := make(map[string]*Bucket)
	for key, value := range ds.values {
		bucket := *value
		values[key] = &bucket
	}
	return &Snapshot{
		values: values,
	}
}

// Global stats and reporting instances.
var (
	GLOBAL_STATS     Stats     = nil
	GLOBAL_REPORTING Reporting = nil
)

var initMutex sync.Mutex = sync.Mutex{}

// Init initializes the global stats and reporting instances.
func Init(stats Stats, reporting Reporting) error {
	initMutex.Lock()
	defer initMutex.Unlock()
	if GLOBAL_STATS == nil && GLOBAL_REPORTING == nil {
		GLOBAL_STATS = stats
		GLOBAL_REPORTING = reporting
		return nil
	} else {
		return errors.ErrStatsAlreadyInitialized
	}
}

// InitDefault initializes the global stats and reporting instances with a DefaultStatsAndReporting instance.
func InitDefault() error {
	defaultStats := NewDefaultStatsAndReporting()
	return Init(defaultStats, defaultStats)
}
