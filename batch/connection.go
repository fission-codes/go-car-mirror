package batch

import (
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/stats"
)

type GenericBatchSourceConnection[I core.BlockId] struct {
	core.Orchestrator[BatchState]
	instrument instrumented.InstrumentationOptions
	stats      stats.Stats
}

func (conn *GenericBatchSourceConnection[I]) Receiver(session *core.SourceSession[I, BatchState]) *SimpleBatchStatusReceiver[I] {
	return NewSimpleBatchStatusReceiver[I](session, conn)
}

func (conn *GenericBatchSourceConnection[I]) Sender(batchSender BatchBlockSender[I], batchSize uint32) core.BlockSender[I] {
	var sender core.BlockSender[I] = NewSimpleBatchBlockSender(batchSender, conn, batchSize)
	if conn.instrument&instrumented.INSTRUMENT_SENDER != 0 {
		sender = instrumented.NewBlockSender(sender, conn.stats.WithContext("BlockSender"))
	}
	return sender
}

func (conn *GenericBatchSourceConnection[I]) Session(store core.BlockStore[I], filter filter.Filter[I], requester bool) *core.SourceSession[I, BatchState] {
	return instrumented.NewSourceSession[I, BatchState](store, filter, conn, conn.stats, conn.instrument, requester)
}

func NewGenericBatchSourceConnection[I core.BlockId](stats stats.Stats, instrument instrumented.InstrumentationOptions) *GenericBatchSourceConnection[I] {

	var orchestrator core.Orchestrator[BatchState] = NewBatchSourceOrchestrator()

	if instrument&instrumented.INSTRUMENT_ORCHESTRATOR != 0 {
		orchestrator = instrumented.NewOrchestrator(orchestrator, stats.WithContext("SourceOrchestrator"))
	}

	return &GenericBatchSourceConnection[I]{
		orchestrator,
		instrument,
		stats,
	}
}

type GenericBatchSinkConnection[I core.BlockId] struct {
	core.Orchestrator[BatchState]
	instrument instrumented.InstrumentationOptions
	stats      stats.Stats
}

func (conn *GenericBatchSinkConnection[I]) Receiver(session *core.SinkSession[I, BatchState]) *SimpleBatchBlockReceiver[I] {
	return NewSimpleBatchBlockReceiver[I](session, conn)
}

// TODO: This really just instruments now, nothing else.
func (conn *GenericBatchSinkConnection[I]) Sender(statusSender core.StatusSender[I]) core.StatusSender[I] {
	if conn.instrument&instrumented.INSTRUMENT_SENDER != 0 {
		statusSender = instrumented.NewStatusSender(statusSender, conn.stats)
	}
	return statusSender
}

func (conn *GenericBatchSinkConnection[I]) Session(store core.BlockStore[I], accumulator core.StatusAccumulator[I], requester bool) *core.SinkSession[I, BatchState] {
	return instrumented.NewSinkSession[I, BatchState](store, accumulator, conn, conn.stats, conn.instrument, requester)
}

func NewGenericBatchSinkConnection[I core.BlockId](stats stats.Stats, instrument instrumented.InstrumentationOptions) *GenericBatchSinkConnection[I] {

	var orchestrator core.Orchestrator[BatchState] = NewBatchSinkOrchestrator()

	if instrument&instrumented.INSTRUMENT_ORCHESTRATOR != 0 {
		orchestrator = instrumented.NewOrchestrator(orchestrator, stats.WithContext("SinkOrchestrator"))
	}

	return &GenericBatchSinkConnection[I]{
		orchestrator,
		instrument,
		stats,
	}
}
