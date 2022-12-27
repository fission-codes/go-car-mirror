package stats

import (
	"context"
	"fmt"

	core "github.com/fission-codes/go-car-mirror/carmirror"

	"github.com/fission-codes/go-car-mirror/filter"
)

// InstrumentedOrchestrator is an Orchestrator that records stats for events.
type InstrumentedOrchestrator[F core.Flags, O core.Orchestrator[F]] struct {
	orchestrator O
	stats        Stats
}

// NewInstrumentedOrchestrator returns a new InstrumentedOrchestrator instance.
func NewInstrumentedOrchestrator[F core.Flags, O core.Orchestrator[F]](orchestrator O, stats Stats) *InstrumentedOrchestrator[F, O] {
	return &InstrumentedOrchestrator[F, O]{
		orchestrator: orchestrator,
		stats:        stats,
	}
}

// Notify calls the underlying orchestrator's Notify method and records stats.
func (io *InstrumentedOrchestrator[F, O]) Notify(event core.SessionEvent) error {
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
type InstrumentedBlockStore[I core.BlockId] struct {
	store core.BlockStore[I]
	stats Stats
}

// NewInstrumentedBlockStore returns a new InstrumentedBlockStore instance.
func NewInstrumentedBlockStore[I core.BlockId](store core.BlockStore[I], stats Stats) *InstrumentedBlockStore[I] {
	return &InstrumentedBlockStore[I]{
		store: store,
		stats: stats,
	}
}

// Get calls the underlying block store's Get method and records stats.
func (ibs *InstrumentedBlockStore[I]) Get(ctx context.Context, id I) (core.Block[I], error) {
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
func (ibs *InstrumentedBlockStore[I]) Add(ctx context.Context, rawBlock core.RawBlock[I]) (core.Block[I], error) {
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
type InstrumentedBlockSender[I core.BlockId] struct {
	sender core.BlockSender[I]
	stats  Stats
}

// NewInstrumentedBlockSender returns a new InstrumentedBlockSender instance.
func NewInstrumentedBlockSender[I core.BlockId](sender core.BlockSender[I], stats Stats) *InstrumentedBlockSender[I] {
	return &InstrumentedBlockSender[I]{
		sender,
		stats,
	}
}

// SendBlock calls the underlying block sender's SendBlock method and records stats.
func (ibs *InstrumentedBlockSender[I]) SendBlock(block core.RawBlock[I]) error {
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
type InstrumentedStatusSender[I core.BlockId] struct {
	sender core.StatusSender[I]
	stats  Stats
}

// NewInstrumentedStatusSender returns a new InstrumentedStatusSender instance.
func NewInstrumentedStatusSender[I core.BlockId](sender core.StatusSender[I], stats Stats) *InstrumentedStatusSender[I] {
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
type InstrumentedStatusReceiver[I core.BlockId, F core.Flags] struct {
	receiver core.StatusReceiver[I, F]
	stats    Stats
}

// NewInstrumentedStatusReceiver returns a new InstrumentedStatusReceiver instance.
func NewInstrumentedStatusReceiver[I core.BlockId, F core.Flags](receiver core.StatusReceiver[I, F], stats Stats) core.StatusReceiver[I, F] {
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
