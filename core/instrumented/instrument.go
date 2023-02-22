package instrumented

import (
	"context"
	"fmt"
	"time"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/filter"
	inf "github.com/fission-codes/go-car-mirror/filter/instrumented"
	"github.com/fission-codes/go-car-mirror/stats"
)

// Orchestrator is an Orchestrator that records stats for events.
type Orchestrator[F core.Flags] struct {
	orchestrator core.Orchestrator[F]
	stats        stats.Stats
}

// NewOrchestrator returns a new Orchestrator instance.
func NewOrchestrator[F core.Flags](orchestrator core.Orchestrator[F], stats stats.Stats) *Orchestrator[F] {
	return &Orchestrator[F]{
		orchestrator: orchestrator,
		stats:        stats,
	}
}

// Notify calls the underlying orchestrator's Notify method and records stats.
func (io *Orchestrator[F]) Notify(event core.SessionEvent) error {
	io.stats.Logger().Debugw("enter", "method", "Notify", "event", event, "state", io.orchestrator.State())
	io.stats.Log(event.String())
	err := io.orchestrator.Notify(event)
	io.stats.Logger().Debugw("exit", "method", "Notify", "event", event, "error", err, "state", io.orchestrator.State())
	return err
}

// State calls the underlying orchestrator's State method and records stats.
func (io *Orchestrator[F]) State() F {
	result := io.orchestrator.State()
	io.stats.Logger().Debugw("exit", "method", "State", "result", result)
	return result
}

// IsClosed calls the underlying orchestrator's IsClosed method and records stats.
func (io *Orchestrator[F]) IsClosed() bool {
	result := io.orchestrator.IsClosed()
	io.stats.Logger().Debugw("exit", "method", "IsClosed", "state", io.orchestrator.State(), "result", result)
	return result
}

func (io *Orchestrator[F]) ShouldClose() bool {
	result := io.orchestrator.ShouldClose()
	io.stats.Logger().Debugw("exit", "method", "ShouldClose", "state", io.orchestrator.State(), "result", result)
	return result
}

// BlockStore is a BlockStore that records stats for events.
type BlockStore[I core.BlockId] struct {
	store core.BlockStore[I]
	stats stats.Stats
}

// NewBlockStore returns a new BlockStore instance.
func NewBlockStore[I core.BlockId](store core.BlockStore[I], stats stats.Stats) *BlockStore[I] {
	return &BlockStore[I]{
		store: store,
		stats: stats,
	}
}

// Get calls the underlying block store's Get method and records stats.
func (ibs *BlockStore[I]) Get(ctx context.Context, id I) (core.Block[I], error) {
	ibs.stats.Logger().Debugw("enter", "method", "Get", "id", id)
	begin := time.Now()
	result, err := ibs.store.Get(ctx, id)
	if err == nil {
		ibs.stats.Log("Get.Ok")
	} else {
		ibs.stats.Log("Get." + err.Error())
		ibs.stats.Logger().Debugw("exit", "method", "Get", "error", err)
	}
	ibs.stats.LogInterval("Get", time.Since(begin))
	return result, err
}

// Has calls the underlying block store's Has method and records stats.
func (ibs *BlockStore[I]) Has(ctx context.Context, id I) (bool, error) {
	ibs.stats.Logger().Debugw("enter", "method", "Has", "id", id)
	begin := time.Now()
	result, err := ibs.store.Has(ctx, id)
	if err == nil {
		ibs.stats.Log(fmt.Sprintf("Has.%v", result))
	} else {
		ibs.stats.Log(fmt.Sprintf("Has.%v", err))
		ibs.stats.Logger().Debugw("exit", "method", "Has", "result", result, "error", err)
	}
	ibs.stats.LogInterval("Has", time.Since(begin))
	return result, err
}

// All calls the underlying block store's All method and records stats.
func (ibs *BlockStore[I]) All(ctx context.Context) (<-chan I, error) {
	ibs.stats.Logger().Debugw("enter", "method", "All")
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
		ibs.stats.Logger().Debugw("exit", "method", "All", "error", err)
	}
	return result, err

}

// Add calls the underlying block store's Add method and records stats.
func (ibs *BlockStore[I]) Add(ctx context.Context, rawBlock core.RawBlock[I]) (core.Block[I], error) {
	ibs.stats.Logger().Debugw("enter", "method", "Add", "id", rawBlock.Id())
	begin := time.Now()
	block, err := ibs.store.Add(ctx, rawBlock)
	if err == nil {
		ibs.stats.Log("Add.Ok")
	} else {
		ibs.stats.Log("Add." + err.Error())
		ibs.stats.Logger().Debugw("exit", "method", "Add", "error", err)
	}
	ibs.stats.LogInterval("Add", time.Since(begin))
	return block, err
}

// BlockSender is a BlockSender that records stats for events.
type BlockSender[I core.BlockId] struct {
	blockSender core.BlockSender[I]
	stats       stats.Stats
}

// NewBlockSender returns a new BlockSender instance.
func NewBlockSender[I core.BlockId](blockSender core.BlockSender[I], stats stats.Stats) *BlockSender[I] {
	return &BlockSender[I]{
		blockSender,
		stats,
	}
}

// SendBlock calls the underlying block sender's SendBlock method and records stats.
func (ibs *BlockSender[I]) SendBlock(block core.RawBlock[I]) error {
	ibs.stats.Logger().Debugw("BlockSender", "method", "SendBlock", "id", block.Id())
	begin := time.Now()
	err := ibs.blockSender.SendBlock(block)
	if err == nil {
		ibs.stats.Log("SendBlock.Ok")
		ibs.stats.LogBytes("SendBlock", uint64(block.Size()))
	} else {
		ibs.stats.Log("SendBlock." + err.Error())
		ibs.stats.Logger().Debugw("BlockSender", "method", "SendBlock", "error", err)
	}
	ibs.stats.LogInterval("SendBlock", time.Since(begin))
	return err
}

// Flush calls the underlying block sender's Flush method and records stats.
func (ibs *BlockSender[I]) Flush() error {
	ibs.stats.Logger().Debugw("BlockSender", "method", "Flush")
	begin := time.Now()
	err := ibs.blockSender.Flush()
	if err == nil {
		ibs.stats.Log("Flush.Ok")
	} else {
		ibs.stats.Log("Flush." + err.Error())
		ibs.stats.Logger().Debugw("BlockSender", "method", "Flush", "error", err)
	}
	ibs.stats.LogInterval("Flush", time.Since(begin))
	return err
}

// Close calls the underlying block sender's Close method and records stats.
func (ibs *BlockSender[I]) Close() error {
	ibs.stats.Logger().Debugw("BlockSender", "method", "Close")
	err := ibs.blockSender.Close()
	if err == nil {
		ibs.stats.Log("Close.Ok")
	} else {
		ibs.stats.Log("Close." + err.Error())
		ibs.stats.Logger().Debugw("BlockSender", "method", "Close", "error", err)
	}
	return err
}

// Len calls the underlying block sender's Len method and records stats.
func (ibs *BlockSender[I]) Len() int {
	ibs.stats.Logger().Debugw("BlockSender", "method", "Len")
	return ibs.blockSender.Len()
}

// StatusSender is a StatusSender that records stats for events.
type StatusSender[I core.BlockId] struct {
	statusSender core.StatusSender[I]
	stats        stats.Stats
}

// NewStatusSender returns a new StatusSender instance.
func NewStatusSender[I core.BlockId](statusSender core.StatusSender[I], stats stats.Stats) *StatusSender[I] {
	return &StatusSender[I]{
		statusSender,
		stats,
	}
}

// SendStatus calls the underlying status sender's SendStatus method and records stats.
func (ibs *StatusSender[I]) SendStatus(have filter.Filter[I], want []I) error {
	ibs.stats.Logger().Debugw("StatusSender", "method", "SendStatus", "haves", have.Count(), "wants", len(want))
	begin := time.Now()
	err := ibs.statusSender.SendStatus(have, want)
	if err == nil {
		ibs.stats.Log("SendStatus.Ok")
	} else {
		ibs.stats.Log("SendStatus." + err.Error())
		ibs.stats.Logger().Debugw("StatusSender", "method", "SendStatus", "error", err)
	}
	ibs.stats.LogInterval("SendStatus", time.Since(begin))
	return err
}

// Close calls the underlying status sender's Close method and records stats.
func (ibs *StatusSender[I]) Close() error {
	ibs.stats.Logger().Debugw("StatusSender", "method", "Close")
	err := ibs.statusSender.Close()
	if err == nil {
		ibs.stats.Log("Close.Ok")
	} else {
		ibs.stats.Log("Close." + err.Error())
		ibs.stats.Logger().Debugw("StatusSender", "method", "Close", "error", err)
	}
	return err
}

// StatusReceiver is a StatusReceiver that records stats for events.
type StatusReceiver[I core.BlockId] struct {
	statusReceiver core.StatusReceiver[I]
	stats          stats.Stats
}

// NewStatusReceiver returns a new StatusReceiver instance.
func NewStatusReceiver[I core.BlockId, F core.Flags](statusReceiver core.StatusReceiver[I], stats stats.Stats) core.StatusReceiver[I] {
	return &StatusReceiver[I]{
		statusReceiver,
		stats,
	}
}

// HandleStatus calls the underlying status receiver's HandleStatus method and records stats.
func (ir *StatusReceiver[I]) HandleStatus(have filter.Filter[I], want []I) {
	ir.stats.Logger().Debugw("StatusReceiver", "method", "HandleStatus", "haves", have.Count(), "wants", len(want))
	ir.statusReceiver.HandleStatus(have, want)
}

type InstrumentationOptions uint8

const (
	INSTRUMENT_STORE InstrumentationOptions = 1 << iota
	INSTRUMENT_ORCHESTRATOR
	INSTRUMENT_FILTER
	INSTRUMENT_SENDER
)

func NewSourceSession[I core.BlockId, F core.Flags](store core.BlockStore[I], filter filter.Filter[I], orchestrator core.Orchestrator[F], stats stats.Stats, options InstrumentationOptions) *core.SourceSession[I, F] {

	if options&INSTRUMENT_STORE != 0 {
		store = NewBlockStore(store, stats.WithContext("SourceStore"))
	}

	if options&INSTRUMENT_FILTER != 0 {
		filter = inf.New(filter, stats.WithContext("SourceFilter"))
	}

	return core.NewSourceSession(store, filter, orchestrator, stats)
}

func NewSinkSession[I core.BlockId, F core.Flags](
	store core.BlockStore[I],
	statusAccumulator core.StatusAccumulator[I],
	orchestrator core.Orchestrator[F],
	stats stats.Stats,
	options InstrumentationOptions,
) *core.SinkSession[I, F] {

	if options&INSTRUMENT_STORE > 0 {
		store = NewBlockStore(store, stats.WithContext("SinkStore"))
	}

	return core.NewSinkSession(store, statusAccumulator, orchestrator, stats)
}
