package carmirror

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/fission-codes/go-car-mirror/filter"
)

// Stats is an interface for recording events.
type Stats interface {
	// Log records an event.
	Log(string)
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

// Logger returns a logger that adds a prefix to all events.
func (ctx *Context) Logger() *zap.SugaredLogger {
	return ctx.logger
}

// Name returns the name of this Stats instance.
func (ctx *Context) Name() string {
	return ctx.name
}

// WithContext returns a new Stats instance that adds a prefix to all events.
func (ctx *Context) WithContext(name string) Stats {
	name = ctx.name + "." + name
	return &Context{
		parent: ctx,
		name:   name,
		logger: ctx.logger.With("for", name),
	}
}

// Snapshot is a snapshot of a Stats instance.
type Snapshot struct {
	values map[string]uint64
}

// Count returns the number of times the given event has been recorded.
func (snap *Snapshot) Count(event string) uint64 {
	return snap.values[event]
}

// Keys returns a sorted list of all events that have been recorded.
func (snap *Snapshot) Keys() []string {
	return maps.Keys(snap.values)
}

// Diff returns a new Snapshot that contains the difference between this Snapshot and the given Snapshot.
func (snap *Snapshot) Diff(other *Snapshot) *Snapshot {
	result := make(map[string]uint64)
	for key, value := range snap.values {
		result[key] = value
	}
	for key, otherValue := range other.values {
		snapValue := result[key]
		if snapValue > otherValue {
			result[key] = snapValue - otherValue
		} else {
			result[key] = otherValue - snapValue
		}
	}
	return &Snapshot{values: result}
}

// Filter returns a new Snapshot that contains only the events that match the given prefix.
func (snap *Snapshot) Filter(prefix string) *Snapshot {
	result := make(map[string]uint64)
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
		log.Infow("snapshot", "event", key, "count", snap.values[key])
	}
}

// Reporting is an interface for reporting stats.
type Reporting interface {
	Count(string) uint64
	Snapshot() *Snapshot
}

// DefaultStatsAndReporting is a Stats and Reporting implementation that records events in memory.
type DefaultStatsAndReporting struct {
	mutex  sync.RWMutex
	values map[string]uint64
	logger *zap.SugaredLogger
}

// NewDefaultStatsAndReporting returns a new DefaultStatsAndReporting instance.
func NewDefaultStatsAndReporting() *DefaultStatsAndReporting {
	return &DefaultStatsAndReporting{
		mutex:  sync.RWMutex{},
		values: make(map[string]uint64),
		logger: &log.SugaredLogger,
	}
}

// Log records an event.
func (ds *DefaultStatsAndReporting) Log(event string) {
	ds.mutex.Lock()
	ds.values[event]++
	ds.mutex.Unlock()
}

// WithContext returns a new Context instance that adds a prefix to all events.
func (ds *DefaultStatsAndReporting) WithContext(name string) Stats {
	return &Context{
		parent: ds,
		name:   name,
		logger: ds.logger.With("for", name),
	}
}

// Logger returns a logger that adds a prefix to all events.
func (ds *DefaultStatsAndReporting) Logger() *zap.SugaredLogger {
	return ds.logger
}

// Name returns the name of this DefaultStatsAndReporting instance.
func (ds *DefaultStatsAndReporting) Name() string {
	return "root"
}

// Count returns the number of times the given event has been recorded.
func (ds *DefaultStatsAndReporting) Count(event string) uint64 {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	return ds.values[event]
}

// Snapshot returns a snapshot of the current DefaultStatsAndReporting instance.
func (ds *DefaultStatsAndReporting) Snapshot() *Snapshot {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	values := make(map[string]uint64)
	for key, value := range ds.values {
		values[key] = value
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

// InstrumentedOrchestrator is an Orchestrator that records stats for events.
type InstrumentedOrchestrator[F Flags, O Orchestrator[F]] struct {
	orchestrator O
	stats        Stats
}

// NewInstrumentedOrchestrator returns a new InstrumentedOrchestrator instance.
func NewInstrumentedOrchestrator[F Flags, O Orchestrator[F]](orchestrator O, stats Stats) *InstrumentedOrchestrator[F, O] {
	return &InstrumentedOrchestrator[F, O]{
		orchestrator: orchestrator,
		stats:        stats,
	}
}

// Notify calls the underlying orchestrator's Notify method and records stats.
func (io *InstrumentedOrchestrator[F, O]) Notify(event SessionEvent) error {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "Notify", "event", event, "state", io.orchestrator.State())
	io.stats.Log(event.String())
	err := io.orchestrator.Notify(event)
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "Notify", "event", event, "result", err, "state", io.orchestrator.State())
	return err
}

// State calls the underlying orchestrator's State method and records stats.
func (io *InstrumentedOrchestrator[F, O]) State() F {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "State")
	result := io.orchestrator.State()
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "result", result)
	return result
}

// ReceiveState calls the underlying orchestrator's ReceiveState method and records stats.
func (io *InstrumentedOrchestrator[F, O]) ReceiveState(state F) error {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "ReceiveState", "state", state)
	err := io.orchestrator.ReceiveState(state)
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "ReceiveState", "err", err)
	return err
}

// IsClosed calls the underlying orchestrator's IsClosed method and records stats.
func (io *InstrumentedOrchestrator[F, O]) IsClosed() bool {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "IsClosed")
	result := io.orchestrator.IsClosed()
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "IsClosed", "result", result)
	return result
}

// InstrumentedBlockStore is a BlockStore that records stats for events.
type InstrumentedBlockStore[I BlockId] struct {
	store BlockStore[I]
	stats Stats
}

// NewInstrumentedBlockStore returns a new InstrumentedBlockStore instance.
func NewInstrumentedBlockStore[I BlockId](store BlockStore[I], stats Stats) *InstrumentedBlockStore[I] {
	return &InstrumentedBlockStore[I]{
		store: store,
		stats: stats,
	}
}

// Get calls the underlying block store's Get method and records stats.
func (ibs *InstrumentedBlockStore[I]) Get(ctx context.Context, id I) (Block[I], error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Get", "id", id)
	result, err := ibs.store.Get(ctx, id)
	if err == nil {
		ibs.stats.Log("Get.Ok")
	} else {
		ibs.stats.Log("Get." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Get", "error", err)
	}
	return result, err
}

// Has calls the underlying block store's Has method and records stats.
func (ibs *InstrumentedBlockStore[I]) Has(ctx context.Context, id I) (bool, error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Has", "id", id)
	result, err := ibs.store.Has(ctx, id)
	if err == nil {
		ibs.stats.Log(fmt.Sprintf("Has.%v", result))
	} else {
		ibs.stats.Log(fmt.Sprintf("Has.%v", err))
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Has", "result", result, "error", err)
	}
	return result, err
}

// All calls the underlying block store's All method and records stats.
func (ibs *InstrumentedBlockStore[I]) All(ctx context.Context) (<-chan I, error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "All")
	blocks, err := ibs.store.All(ctx)
	result := make(chan I)
	if err == nil {
		go func(blocks <-chan I) {
			for block := range blocks {
				ibs.stats.Log("All.chan")
				result <- block
			}

		}(blocks)
	} else {
		ibs.stats.Log(fmt.Sprintf("All.%v", err))
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "All", "error", err)
	}
	return result, err

}

// Add calls the underlying block store's Add method and records stats.
func (ibs *InstrumentedBlockStore[I]) Add(ctx context.Context, rawBlock RawBlock[I]) (Block[I], error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Add", "id", rawBlock.Id())
	block, err := ibs.store.Add(ctx, rawBlock)
	if err == nil {
		ibs.stats.Log("Add.Ok")
	} else {
		ibs.stats.Log("Add." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Add", "error", err)
	}
	return block, err
}

// InstrumentedBlockSender is a BlockSender that records stats for events.
type InstrumentedBlockSender[I BlockId] struct {
	sender BlockSender[I]
	stats  Stats
}

// NewInstrumentedBlockSender returns a new InstrumentedBlockSender instance.
func NewInstrumentedBlockSender[I BlockId](sender BlockSender[I], stats Stats) *InstrumentedBlockSender[I] {
	return &InstrumentedBlockSender[I]{
		sender,
		stats,
	}
}

// SendBlock calls the underlying block sender's SendBlock method and records stats.
func (ibs *InstrumentedBlockSender[I]) SendBlock(block RawBlock[I]) error {
	ibs.stats.Logger().Debugw("InstrumentedBlockSender", "method", "SendBlock", "id", block.Id())
	err := ibs.sender.SendBlock(block)
	if err == nil {
		ibs.stats.Log("SendBlock.Ok")
	} else {
		ibs.stats.Log("SendBlock." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockSender", "method", "SendBlock", "error", err)
	}
	return err
}

// Flush calls the underlying block sender's Flush method and records stats.
func (ibs *InstrumentedBlockSender[I]) Flush() error {
	ibs.stats.Logger().Debugw("InstrumentedBlockSender", "method", "Flush")
	err := ibs.sender.Flush()
	if err == nil {
		ibs.stats.Log("Flush.Ok")
	} else {
		ibs.stats.Log("Flush." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockSender", "method", "Flush", "error", err)
	}
	return err
}

// Close calls the underlying block sender's Close method and records stats.
func (ibs *InstrumentedBlockSender[I]) Close() error {
	ibs.stats.Logger().Debugw("InstrumentedBlockSender", "method", "Close")
	err := ibs.sender.Close()
	if err == nil {
		ibs.stats.Log("Close.Ok")
	} else {
		ibs.stats.Log("Close." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockSender", "method", "Close", "error", err)
	}
	return err
}

// InstrumentedStatusSender is a StatusSender that records stats for events.
type InstrumentedStatusSender[I BlockId] struct {
	sender StatusSender[I]
	stats  Stats
}

// NewInstrumentedStatusSender returns a new InstrumentedStatusSender instance.
func NewInstrumentedStatusSender[I BlockId](sender StatusSender[I], stats Stats) *InstrumentedStatusSender[I] {
	return &InstrumentedStatusSender[I]{
		sender,
		stats,
	}
}

// SendStatus calls the underlying status sender's SendStatus method and records stats.
func (ibs *InstrumentedStatusSender[I]) SendStatus(have filter.Filter[I], want []I) error {
	ibs.stats.Logger().Debugw("InstrumentedStatusSender", "method", "SendStatus", "haves", have.Count(), "wants", len(want))
	err := ibs.sender.SendStatus(have, want)
	if err == nil {
		ibs.stats.Log("SendStatus.Ok")
	} else {
		ibs.stats.Log("SendStatus." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedStatusSender", "method", "SendStatus", "error", err)
	}
	return err
}

// Close calls the underlying status sender's Close method and records stats.
func (ibs *InstrumentedStatusSender[I]) Close() error {
	ibs.stats.Logger().Debugw("InstrumentedStatusSender", "method", "Close")
	err := ibs.sender.Close()
	if err == nil {
		ibs.stats.Log("Close.Ok")
	} else {
		ibs.stats.Log("Close." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedStatusSender", "method", "Close", "error", err)
	}
	return err
}

// InstrumentedStatusReceiver is a StatusReceiver that records stats for events.
type InstrumentedStatusReceiver[I BlockId, F Flags] struct {
	receiver StatusReceiver[I, F]
	stats    Stats
}

// NewInstrumentedStatusReceiver returns a new InstrumentedStatusReceiver instance.
func NewInstrumentedStatusReceiver[I BlockId, F Flags](receiver StatusReceiver[I, F], stats Stats) StatusReceiver[I, F] {
	return &InstrumentedStatusReceiver[I, F]{
		receiver,
		stats,
	}
}

// HandleStatus calls the underlying status receiver's HandleStatus method and records stats.
func (ir *InstrumentedStatusReceiver[I, F]) HandleStatus(have filter.Filter[I], want []I) {
	ir.stats.Logger().Debugw("InstrumentedStatusReceiver", "method", "HandleStatus", "haves", have.Count(), "wants", len(want))
	ir.receiver.HandleStatus(have, want)
}

// HandleState calls the underlying status receiver's HandleState method and records stats.
func (ir *InstrumentedStatusReceiver[I, F]) HandleState(state F) {
	ir.stats.Logger().Debugw("InstrumentedStatusReceiver", "method", "HandleStatus", "state", state)
	ir.receiver.HandleState(state)
}
