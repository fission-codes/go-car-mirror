package batch

import (
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/fission-codes/go-car-mirror/stats"
)

type GenericBatchSourceConnection[I core.BlockId, R core.BlockIdRef[I]] struct {
	core.Orchestrator[BatchState]
	instrument           instrumented.InstrumentationOptions
	stats                stats.Stats
	messages             chan *messages.BlocksMessage[I, R]
	maxBlocksPerRound    uint32
	maxBlocksPerColdCall uint32
}

func NewGenericBatchSourceConnection[I core.BlockId, R core.BlockIdRef[I]](stats stats.Stats, instrument instrumented.InstrumentationOptions, maxBlocksPerRound uint32, maxBlocksPerColdCall uint32, requester bool) *GenericBatchSourceConnection[I, R] {
	var orchestrator core.Orchestrator[BatchState] = NewBatchSourceOrchestrator(requester)

	if instrument&instrumented.INSTRUMENT_ORCHESTRATOR != 0 {
		orchestrator = instrumented.NewOrchestrator(orchestrator, stats.WithContext("SourceOrchestrator"))
	}

	return &GenericBatchSourceConnection[I, R]{
		orchestrator,
		instrument,
		stats,
		make(chan *messages.BlocksMessage[I, R]),
		maxBlocksPerRound,
		maxBlocksPerColdCall,
	}
}

// IsRequester returns true if the connection is a requester.
func (conn *GenericBatchSourceConnection[I, R]) IsRequester() bool {
	return conn.Orchestrator.IsRequester()
}

func (conn *GenericBatchSourceConnection[I, R]) Receiver(session *core.SourceSession[I, BatchState]) *SimpleBatchStatusReceiver[I] {
	return NewSimpleBatchStatusReceiver[I](session, conn)
}

func (conn *GenericBatchSourceConnection[I, R]) Sender(batchSender BatchBlockSender[I], maxBlocksPerRound uint32) core.BlockSender[I] {
	var sender core.BlockSender[I] = NewSimpleBatchBlockSender(batchSender, conn, maxBlocksPerRound)
	if conn.instrument&instrumented.INSTRUMENT_SENDER != 0 {
		sender = instrumented.NewBlockSender(sender, conn.stats.WithContext("BlockSender"))
	}
	return sender
}

func (conn *GenericBatchSourceConnection[I, R]) Session(store core.BlockStore[I], filter filter.Filter[I], requester bool) *core.SourceSession[I, BatchState] {
	return instrumented.NewSourceSession[I, BatchState](store, filter, conn, conn.stats, conn.instrument, conn.maxBlocksPerRound, conn.maxBlocksPerColdCall, requester)
}

func (conn *GenericBatchSourceConnection[I, R]) DeferredSender(maxBlocksPerRound uint32) core.BlockSender[I] {
	return conn.Sender(&ResponseBatchBlockSender[I, R]{conn.messages, conn}, maxBlocksPerRound)
}

func (conn *GenericBatchSourceConnection[I, R]) DeferredBatchSender() BatchBlockSender[I] {
	return &ResponseBatchBlockSender[I, R]{conn.messages, conn}
}

func (conn *GenericBatchSourceConnection[I, R]) PendingResponse() *messages.BlocksMessage[I, R] {
	return <-conn.messages
}

type GenericBatchSinkConnection[I core.BlockId, R core.BlockIdRef[I]] struct {
	core.Orchestrator[BatchState]
	instrument        instrumented.InstrumentationOptions
	stats             stats.Stats
	messages          chan *messages.StatusMessage[I, R]
	maxBlocksPerRound uint32
}

func NewGenericBatchSinkConnection[I core.BlockId, R core.BlockIdRef[I]](stats stats.Stats, instrument instrumented.InstrumentationOptions, maxBlocksPerRound uint32, requester bool) *GenericBatchSinkConnection[I, R] {

	var orchestrator core.Orchestrator[BatchState] = NewBatchSinkOrchestrator(requester)

	if instrument&instrumented.INSTRUMENT_ORCHESTRATOR != 0 {
		orchestrator = instrumented.NewOrchestrator(orchestrator, stats.WithContext("SinkOrchestrator"))
	}

	return &GenericBatchSinkConnection[I, R]{
		orchestrator,
		instrument,
		stats,
		make(chan *messages.StatusMessage[I, R]),
		maxBlocksPerRound,
	}
}

func (conn *GenericBatchSinkConnection[I, R]) Receiver(session *core.SinkSession[I, BatchState]) *SimpleBatchBlockReceiver[I] {
	return NewSimpleBatchBlockReceiver[I](session, conn)
}

// TODO: This really just instruments now, nothing else.
func (conn *GenericBatchSinkConnection[I, R]) Sender(statusSender core.StatusSender[I]) core.StatusSender[I] {
	if conn.instrument&instrumented.INSTRUMENT_SENDER != 0 {
		statusSender = instrumented.NewStatusSender(statusSender, conn.stats)
	}
	return statusSender
}

func (conn *GenericBatchSinkConnection[I, R]) Session(store core.BlockStore[I], accumulator core.StatusAccumulator[I], requester bool) *core.SinkSession[I, BatchState] {
	return instrumented.NewSinkSession[I, BatchState](store, accumulator, conn, conn.stats, conn.instrument, conn.maxBlocksPerRound, requester)
}

func (conn *GenericBatchSinkConnection[I, R]) DeferredBatchSender() core.StatusSender[I] {
	return &ResponseStatusSender[I, R]{conn.messages, conn}
}

func (conn *GenericBatchSinkConnection[I, R]) DeferredSender() core.StatusSender[I] {
	return conn.Sender(&ResponseStatusSender[I, R]{conn.messages, conn})
}

func (conn *GenericBatchSinkConnection[I, R]) PendingResponse() *messages.StatusMessage[I, R] {
	return <-conn.messages
}

type ResponseBatchBlockSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	messages     chan<- *messages.BlocksMessage[I, R]
	orchestrator core.Orchestrator[BatchState]
}

func (bbs *ResponseBatchBlockSender[I, R]) SendList(blocks []core.RawBlock[I]) error {
	if err := bbs.orchestrator.Notify(core.BEGIN_FLUSH); err != nil {
		return err
	}
	log.Debugw("ResponseBatchBlockSender - enter", "method", "SendList", "blocks", len(blocks))
	bbs.messages <- messages.NewBlocksMessage[I, R](blocks)
	log.Debugw("ResponseBatchBlockSender - exit", "method", "SendList")
	if err := bbs.orchestrator.Notify(core.END_FLUSH); err != nil {
		return err
	}
	return nil
}

func (bbs *ResponseBatchBlockSender[I, R]) Close() error {
	close(bbs.messages)
	return nil
}

type ResponseStatusSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	messages     chan<- *messages.StatusMessage[I, R]
	orchestrator core.Orchestrator[BatchState]
}

func (ss *ResponseStatusSender[I, R]) SendStatus(have filter.Filter[I], want []I) error {
	if err := ss.orchestrator.Notify(core.BEGIN_FLUSH); err != nil {
		return err
	}
	log.Debugw("enter", "object", "ResponseStatusSender", "method", "SendStatus", "have", have.Count(), "want", len(want))
	ss.messages <- messages.NewStatusMessage[I, R](have, want)
	log.Debugw("exit", "object", "ResponseStatusSender", "method", "SendStatus")
	if err := ss.orchestrator.Notify(core.END_FLUSH); err != nil {
		return err
	}
	return nil
}

func (ss *ResponseStatusSender[I, R]) Close() error {
	close(ss.messages)
	return nil
}
