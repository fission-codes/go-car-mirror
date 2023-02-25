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
	store             core.BlockStore[I]
	sinkSessions      *util.SynchronizedMap[SessionId, *SinkSessionData[I]]
	filterAllocator   func() filter.Filter[I]
	batchStatusSender BatchStatusSender[I]
}

func NewSinkResponder[I core.BlockId](store core.BlockStore[I], config Config, batchStatusSender BatchStatusSender[I]) *SinkResponder[I] {
	return &SinkResponder[I]{
		store,
		util.NewSynchronizedMap[SessionId, *SinkSessionData[I]](),
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
