package carmirror

import (
	"fmt"
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

var LOG = zap.S()

type Stats interface {
	Log(string)
	WithContext(string) Stats
}

type Context struct {
	parent  Stats
	context string
}

func (ctx *Context) Log(event string) {
	ctx.parent.Log(ctx.context + "." + event)
}

func (ctx *Context) WithContext(context string) Stats {
	return &Context{
		parent:  ctx,
		context: context,
	}
}

type Snapshot struct {
	values map[string]uint64
}

func (snap *Snapshot) GetCount(event string) uint64 {
	return snap.values[event]
}

func (snap *Snapshot) Keys() []string {
	return maps.Keys(snap.values)
}

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

func (snap *Snapshot) Filter(prefix string) *Snapshot {
	result := make(map[string]uint64)
	for key, value := range snap.values {
		if strings.HasPrefix(key, prefix) {
			result[key] = value
		}
	}
	return &Snapshot{values: result}
}

func (snap *Snapshot) Write(log *zap.SugaredLogger) {
	for key, value := range snap.values {
		log.Infow("snapshot", "event", key, "count", value)
	}
}

type Reporting interface {
	GetCount(string) uint64
	Snapshot() *Snapshot
}

type DefaultStatsAndReporting struct {
	mutex  sync.RWMutex
	values map[string]uint64
}

func NewDefaultStatsAndReporting() *DefaultStatsAndReporting {
	return &DefaultStatsAndReporting{
		mutex:  sync.RWMutex{},
		values: make(map[string]uint64),
	}
}

func (ds *DefaultStatsAndReporting) Log(event string) {
	ds.mutex.Lock()
	ds.values[event]++
	ds.mutex.Unlock()
}

func (ds *DefaultStatsAndReporting) WithContext(context string) Stats {
	return &Context{
		parent:  ds,
		context: context,
	}
}

func (ds *DefaultStatsAndReporting) GetCount(event string) uint64 {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()
	return ds.values[event]
}

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

var GLOBAL_STATS Stats = nil
var GLOBAL_REPORTING Reporting = nil
var initMutex sync.Mutex = sync.Mutex{}

func Init(stats Stats, reporting Reporting) error {
	initMutex.Lock()
	defer initMutex.Unlock()
	if GLOBAL_STATS == nil && GLOBAL_REPORTING == nil {
		GLOBAL_STATS = stats
		GLOBAL_REPORTING = reporting
		return nil
	} else {
		return errors.StatsAlreadyInitialized
	}
}

func InitDefault() error {
	defaultStats := NewDefaultStatsAndReporting()
	return Init(defaultStats, defaultStats)
}

type InstrumentedOrchestrator[F Flags, O Orchestrator[F]] struct {
	orchestrator O
	stats        Stats
}

func NewInstrumentedOrchestrator[F Flags, O Orchestrator[F]](orchestrator O, stats Stats) *InstrumentedOrchestrator[F, O] {
	return &InstrumentedOrchestrator[F, O]{
		orchestrator: orchestrator,
		stats:        stats,
	}
}

func (io *InstrumentedOrchestrator[F, O]) Notify(event SessionEvent) error {
	LOG.Debugf("InstrumentedOrchestrator", "method", "Notify", "event", event)
	io.stats.Log(event.String())
	return io.orchestrator.Notify(event)
}

func (io *InstrumentedOrchestrator[F, O]) GetState() (F, error) {
	LOG.Debugf("InstrumentedOrchestrator", "method", "GetState")
	result, err := io.orchestrator.GetState()
	LOG.Debugf("InstrumentedOrchestrator", "result", result, "err", err)
	return result, err
}

func (io *InstrumentedOrchestrator[F, O]) ReceiveState(state F) error {
	LOG.Debugf("InstrumentedOrchestrator", "method", "ReceiveState", "state", state)
	err := io.orchestrator.ReceiveState(state)
	LOG.Debugf("InstrumentedOrchestrator", "method", "ReceiveState", "err", err)
	return err
}

func (io *InstrumentedOrchestrator[F, O]) IsClosed() (bool, error) {
	LOG.Debugf("InstrumentedOrchestrator", "method", "IsClosed")
	result, err := io.orchestrator.IsClosed()
	LOG.Debugf("InstrumentedOrchestrator", "method", "IsClosed", "result", result, "err", err)
	return result, err
}

type InstrumentedBlockStore[I BlockId] struct {
	store BlockStore[I]
	stats Stats
}

func NewInstrumentedBlockStore[I BlockId](store BlockStore[I], stats Stats) *InstrumentedBlockStore[I] {
	return &InstrumentedBlockStore[I]{
		store: store,
		stats: stats,
	}
}

func (ibs *InstrumentedBlockStore[I]) Get(id I) (Block[I], error) {
	LOG.Debugf("InstrumentedBlockStore", "method", "Get", "id", id)
	result, err := ibs.store.Get(id)
	if err == nil {
		ibs.stats.Log("Get.Ok")
	} else {
		ibs.stats.Log("Get." + err.Error())
		LOG.Debugf("InstrumentedBlockStore", "method", "Get", "error", err)
	}
	return result, err
}

func (ibs *InstrumentedBlockStore[I]) Has(id I) (bool, error) {
	LOG.Debugf("InstrumentedBlockStore", "method", "Has", "id", id)
	result, err := ibs.store.Has(id)
	if err == nil {
		ibs.stats.Log(fmt.Sprintf("Has.%v", result))
	} else {
		ibs.stats.Log(fmt.Sprintf("Has.%v", err))
		LOG.Debugf("InstrumentedBlockStore", "method", "Has", "result", result, "error", err)
	}
	return result, err
}

func (ibs *InstrumentedBlockStore[I]) All() (<-chan I, error) {
	LOG.Debugf("InstrumentedBlockStore", "method", "All")
	blocks, err := ibs.store.All()
	result := make(chan I)
	if err == nil {
		ibs.stats.Log(fmt.Sprintf("Has.%v", result))
		go func(blocks <-chan I) {
			for block := range blocks {
				ibs.stats.Log(fmt.Sprintf("All.chan"))
				result <- block
			}

		}(blocks)
	} else {
		ibs.stats.Log(fmt.Sprintf("All.%v", err))
		LOG.Debugf("InstrumentedBlockStore", "method", "All", "error", err)
	}
	return result, err

}
