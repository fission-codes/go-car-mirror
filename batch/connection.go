package batch

import (
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/stats"
)

type BatchStatusSender[I core.BlockId] interface {
	// SendStatus sends status and state
	SendStatus(state BatchState, have filter.Filter[I], want []I) error
	Close() error
}

type SimpleBatchStatusSender[I core.BlockId] struct {
	sender       BatchStatusSender[I]
	orchestrator core.Orchestrator[BatchState]
}

func NewSimpleBatchStatusSender[I core.BlockId](sender BatchStatusSender[I], orchestrator core.Orchestrator[BatchState]) *SimpleBatchStatusSender[I] {
	return &SimpleBatchStatusSender[I]{sender, orchestrator}
}

func (snd *SimpleBatchStatusSender[I]) SendStatus(have filter.Filter[I], want []I) error {
	return snd.sender.SendStatus(snd.orchestrator.State(), have, want)
}

func (snd *SimpleBatchStatusSender[I]) Close() error {
	return snd.sender.Close()
}

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

func (conn *GenericBatchSourceConnection[I]) Session(store core.BlockStore[I], filter filter.Filter[I]) *core.SourceSession[I, BatchState] {
	return instrumented.NewSourceSession[I, BatchState](store, filter, conn, conn.stats, conn.instrument)
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

func (conn *GenericBatchSinkConnection[I]) Sender(batchSender BatchStatusSender[I]) core.StatusSender[I] {
	var sender core.StatusSender[I] = NewSimpleBatchStatusSender(batchSender, conn)
	if conn.instrument&instrumented.INSTRUMENT_SENDER != 0 {
		sender = instrumented.NewStatusSender(sender, conn.stats)
	}
	return sender
}

func (conn *GenericBatchSinkConnection[I]) Session(store core.BlockStore[I], accumulator core.StatusAccumulator[I]) *core.SinkSession[I, BatchState] {
	return instrumented.NewSinkSession[I, BatchState](store, accumulator, conn, conn.stats, conn.instrument)
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
