package batch

import (
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/stats"
	"github.com/fission-codes/go-car-mirror/util"
)

type SessionId string

type SinkSessionData[I core.BlockId] struct {
	// conn is different for http.  Is a func the way to go or generics?
	// Just using what we need in test to start with.
	conn    *GenericBatchSinkConnection[I]
	Session *core.SinkSession[I, BatchState]
}

type SinkResponder[I core.BlockId] struct {
	// for responder sessions, we don't need to do anything to them other then Run?  No, we might need to cancel.
	store        core.BlockStore[I]
	sinkSessions *util.SynchronizedMap[SessionId, *SinkSessionData[I]]
	// TODO: This gets invoked and passed to SimpleStatusAccumulator, which has mutex, so this allocator should just return a non-sync filter
	filterAllocator   func() filter.Filter[I]
	batchStatusSender BatchStatusSender[I]
}

func NewSinkResponder[I core.BlockId](store core.BlockStore[I], config Config, batchStatusSender BatchStatusSender[I]) *SinkResponder[I] {
	return &SinkResponder[I]{
		store,
		util.NewSynchronizedMap[SessionId, *SinkSessionData[I]](),
		// NewBloomAllocator isn't returning a synchronized filter
		NewBloomAllocator[I](&config),
		batchStatusSender,
	}
}

func (sr *SinkResponder[I]) newSinkConnection() *GenericBatchSinkConnection[I] {
	return NewGenericBatchSinkConnection[I](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)
}

func (sr *SinkResponder[I]) SinkSessionData(sessionId SessionId) *SinkSessionData[I] {
	sessionData := sr.sinkSessions.GetOrInsert(
		sessionId,
		func() *SinkSessionData[I] {
			return sr.startSinkSession(sessionId)
		},
	)

	return sessionData
}

func (sr *SinkResponder[I]) SinkSession(sessionId SessionId) *core.SinkSession[I, BatchState] {
	sessionData := sr.SinkSessionData(sessionId)
	return sessionData.Session
}

func (sr *SinkResponder[I]) SinkConnection(sessionId SessionId) *GenericBatchSinkConnection[I] {
	sessionData := sr.SinkSessionData(sessionId)
	return sessionData.conn
}

func (sr *SinkResponder[I]) Receiver(sessionId SessionId) *SimpleBatchBlockReceiver[I] {
	sessionData := sr.SinkSessionData(sessionId)
	return sessionData.conn.Receiver(sessionData.Session)
}

func (sr *SinkResponder[I]) startSinkSession(sessionId SessionId) *SinkSessionData[I] {

	sinkConnection := sr.newSinkConnection()

	newSession := sinkConnection.Session(
		sr.store,
		// Since SimpleStatusAccumulator uses mutex locks, the filter allocator just returns a non-sync filter
		core.NewSimpleStatusAccumulator(sr.filterAllocator()),
	)

	// TODO: validate that we have a batch status sender first

	statusSender := sinkConnection.Sender(sr.batchStatusSender)

	go func() {
		newSession.Run(statusSender)
		// statusSender.Close()
		sr.sinkSessions.Remove(sessionId)
	}()

	// Wait for the session to start
	<-newSession.Started()

	return &SinkSessionData[I]{
		sinkConnection,
		newSession,
	}
}

func (sr *SinkResponder[I]) SetBatchStatusSender(batchStatusSender BatchStatusSender[I]) {
	sr.batchStatusSender = batchStatusSender
}

// Need to get sink session, start it, allow dynamic looking up of its receiver

type Config struct {
	MaxBatchSize  uint32
	BloomCapacity uint
	BloomFunction uint64
	Instrument    instrumented.InstrumentationOptions
}

func DefaultConfig() Config {
	return Config{
		MaxBatchSize:  32,
		BloomCapacity: 1024,
		BloomFunction: 2,
		Instrument:    0,
	}
}

func NewBloomAllocator[I core.BlockId](config *Config) func() filter.Filter[I] {
	return func() filter.Filter[I] {
		filter, err := filter.TryNewBloomFilter[I](config.BloomCapacity, config.BloomFunction)
		if err != nil {
			log.Errorf("Invalid hash function specified %v", config.BloomFunction)
		}
		return filter
	}
}

type SourceSessionData[I core.BlockId] struct {
	// conn is different for http.  Is a func the way to go or generics?
	// Just using what we need in test to start with.
	conn    *GenericBatchSourceConnection[I]
	Session *core.SourceSession[I, BatchState]
}

type SourceResponder[I core.BlockId] struct {
	// for responder sessions, we don't need to do anything to them other then Run?  No, we might need to cancel.
	store          core.BlockStore[I]
	sourceSessions *util.SynchronizedMap[SessionId, *SourceSessionData[I]]
	// TODO: SourceSession does not sync wrapping of the filter, ...
	filterAllocator  func() filter.Filter[I]
	batchBlockSender BatchBlockSender[I]
	batchSize        uint32
}

func NewSourceResponder[I core.BlockId](store core.BlockStore[I], config Config, batchBlockSender BatchBlockSender[I], batchSize uint32) *SourceResponder[I] {
	return &SourceResponder[I]{
		store,
		util.NewSynchronizedMap[SessionId, *SourceSessionData[I]](),
		NewBloomAllocator[I](&config),
		batchBlockSender,
		batchSize,
	}
}

func (sr *SourceResponder[I]) newSourceConnection() *GenericBatchSourceConnection[I] {
	return NewGenericBatchSourceConnection[I](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)
}

func (sr *SourceResponder[I]) SourceSessionData(sessionId SessionId) *SourceSessionData[I] {
	sessionData := sr.sourceSessions.GetOrInsert(
		sessionId,
		func() *SourceSessionData[I] {
			return sr.startSourceSession(sessionId)
		},
	)

	return sessionData
}

func (sr *SourceResponder[I]) SourceSession(sessionId SessionId) *core.SourceSession[I, BatchState] {
	sessionData := sr.SourceSessionData(sessionId)
	return sessionData.Session
}

func (sr *SourceResponder[I]) SourceConnection(sessionId SessionId) *GenericBatchSourceConnection[I] {
	sessionData := sr.SourceSessionData(sessionId)
	return sessionData.conn
}

func (sr *SourceResponder[I]) Receiver(sessionId SessionId) *SimpleBatchStatusReceiver[I] {
	sessionData := sr.SourceSessionData(sessionId)
	return sessionData.conn.Receiver(sessionData.Session)
}

func (sr *SourceResponder[I]) startSourceSession(sessionId SessionId) *SourceSessionData[I] {

	sourceConnection := sr.newSourceConnection()

	newSession := sourceConnection.Session(
		sr.store,
		// TODO: SourceSession has no mutex locks for filter, so we need to wrap.  Inconsistent though and really should be cleaned up.
		filter.NewSynchronizedFilter(sr.filterAllocator()),
	)

	blockSender := sourceConnection.Sender(sr.batchBlockSender, sr.batchSize)

	go func() {
		newSession.Run(blockSender)
		// blockSender.Close()
		sr.sourceSessions.Remove(sessionId)
	}()

	// Wait for the session to start
	<-newSession.Started()

	return &SourceSessionData[I]{
		sourceConnection,
		newSession,
	}
}

func (sr *SourceResponder[I]) SetBatchBlockSender(batchBlockSender BatchBlockSender[I]) {
	sr.batchBlockSender = batchBlockSender
}
