package carmirror

import (
	"fmt"
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/fission-codes/go-car-mirror/filter"
)

type Stats interface {
	Log(string)
	WithContext(string) Stats
	Logger() *zap.SugaredLogger
	Name() string
}

type Context struct {
	parent Stats
	name   string
	logger *zap.SugaredLogger
}

func (ctx *Context) Log(event string) {
	ctx.parent.Log(ctx.name + "." + event)
}

func (ctx *Context) Logger() *zap.SugaredLogger {
	return ctx.logger
}

func (ctx *Context) Name() string {
	return ctx.name
}

func (ctx *Context) WithContext(name string) Stats {
	name = ctx.name + "." + name
	return &Context{
		parent: ctx,
		name:   name,
		logger: ctx.logger.With("for", name),
	}
}

type Snapshot struct {
	values map[string]uint64
}

func (snap *Snapshot) Count(event string) uint64 {
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
	Count(string) uint64
	Snapshot() *Snapshot
}

type DefaultStatsAndReporting struct {
	mutex  sync.RWMutex
	values map[string]uint64
	logger *zap.SugaredLogger
}

func NewDefaultStatsAndReporting() *DefaultStatsAndReporting {
	return &DefaultStatsAndReporting{
		mutex:  sync.RWMutex{},
		values: make(map[string]uint64),
		logger: zap.S(),
	}
}

func (ds *DefaultStatsAndReporting) Log(event string) {
	ds.mutex.Lock()
	ds.values[event]++
	ds.mutex.Unlock()
}

func (ds *DefaultStatsAndReporting) WithContext(name string) Stats {
	return &Context{
		parent: ds,
		name:   name,
		logger: ds.logger.With("for", name),
	}
}

func (ds *DefaultStatsAndReporting) Logger() *zap.SugaredLogger {
	return ds.logger
}

func (ds *DefaultStatsAndReporting) Name() string {
	return "root"
}

func (ds *DefaultStatsAndReporting) Count(event string) uint64 {
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
		return errors.ErrStatsAlreadyInitialized
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
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "Notify", "event", event, "state", io.orchestrator.State())
	io.stats.Log(event.String())
	err := io.orchestrator.Notify(event)
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "Notify", "result", err, "state", io.orchestrator.State())
	return err
}

func (io *InstrumentedOrchestrator[F, O]) State() F {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "State")
	result := io.orchestrator.State()
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "result", result)
	return result
}

func (io *InstrumentedOrchestrator[F, O]) ReceiveState(state F) error {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "ReceiveState", "state", state)
	err := io.orchestrator.ReceiveState(state)
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "ReceiveState", "err", err)
	return err
}

func (io *InstrumentedOrchestrator[F, O]) IsClosed() bool {
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "IsClosed")
	result := io.orchestrator.IsClosed()
	io.stats.Logger().Debugw("InstrumentedOrchestrator", "method", "IsClosed", "result", result)
	return result
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
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Get", "id", id)
	result, err := ibs.store.Get(id)
	if err == nil {
		ibs.stats.Log("Get.Ok")
	} else {
		ibs.stats.Log("Get." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Get", "error", err)
	}
	return result, err
}

func (ibs *InstrumentedBlockStore[I]) Has(id I) (bool, error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Has", "id", id)
	result, err := ibs.store.Has(id)
	if err == nil {
		ibs.stats.Log(fmt.Sprintf("Has.%v", result))
	} else {
		ibs.stats.Log(fmt.Sprintf("Has.%v", err))
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Has", "result", result, "error", err)
	}
	return result, err
}

func (ibs *InstrumentedBlockStore[I]) All() (<-chan I, error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "All")
	blocks, err := ibs.store.All()
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

func (ibs *InstrumentedBlockStore[I]) Add(raw_block RawBlock[I]) (Block[I], error) {
	ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Add", "id", raw_block.Id())
	block, err := ibs.store.Add(raw_block)
	if err == nil {
		ibs.stats.Log("Add.Ok")
	} else {
		ibs.stats.Log("Add." + err.Error())
		ibs.stats.Logger().Debugw("InstrumentedBlockStore", "method", "Add", "error", err)
	}
	return block, err
}

type InstrumentedBlockSender[I BlockId] struct {
	sender BlockSender[I]
	stats  Stats
}

func NewInstrumentedBlockSender[I BlockId](sender BlockSender[I], stats Stats) *InstrumentedBlockSender[I] {
	return &InstrumentedBlockSender[I]{
		sender,
		stats,
	}
}

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

type InstrumentedStatusSender[I BlockId] struct {
	sender StatusSender[I]
	stats  Stats
}

func NewInstrumentedStatusSender[I BlockId](sender StatusSender[I], stats Stats) *InstrumentedStatusSender[I] {
	return &InstrumentedStatusSender[I]{
		sender,
		stats,
	}
}

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

type InstrumentedStatusReceiver[I BlockId, F Flags] struct {
	receiver StatusReceiver[I, F]
	stats    Stats
}

func NewInstrumentedStatusReceiver[I BlockId, F Flags](receiver StatusReceiver[I, F], stats Stats) StatusReceiver[I, F] {
	return &InstrumentedStatusReceiver[I, F]{
		receiver,
		stats,
	}
}

func (ir *InstrumentedStatusReceiver[I, F]) HandleStatus(have filter.Filter[I], want []I) {
	ir.stats.Logger().Debugw("InstrumentedStatusReceiver", "method", "HandleStatus", "haves", have.Count(), "wants", len(want))
	ir.receiver.HandleStatus(have, want)
}

func (ir *InstrumentedStatusReceiver[I, F]) HandleState(state F) {
	ir.stats.Logger().Debugw("InstrumentedStatusReceiver", "method", "HandleStatus", "state", state)
	ir.receiver.HandleState(state)
}
