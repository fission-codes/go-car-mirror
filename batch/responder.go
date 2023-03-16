package batch

import (
	"fmt"
	"sync"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/stats"
	"github.com/fission-codes/go-car-mirror/util"
)

type SessionId string

type SinkSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	// conn is different for http.  Is a func the way to go or generics?
	// Just using what we need in test to start with.
	conn    *GenericBatchSinkConnection[I, R]
	Session *core.SinkSession[I, BatchState]
}

type SinkResponder[I core.BlockId, R core.BlockIdRef[I]] struct {
	// for responder sessions, we don't need to do anything to them other then Run?  No, we might need to cancel.
	store        core.BlockStore[I]
	sinkSessions *util.SynchronizedMap[SessionId, *SinkSessionData[I, R]]
	// TODO: This gets invoked and passed to SimpleStatusAccumulator, which has mutex, so this allocator should just return a non-sync filter
	filterAllocator      func() filter.Filter[I]
	statusSender         core.StatusSender[I]
	maxBlocksPerRound    uint32
	maxBlocksPerColdCall uint32
}

func NewSinkResponder[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config, statusSender core.StatusSender[I]) *SinkResponder[I, R] {
	return &SinkResponder[I, R]{
		store,
		util.NewSynchronizedMap[SessionId, *SinkSessionData[I, R]](),
		// NewBloomAllocator isn't returning a synchronized filter
		NewBloomAllocator[I](&config),
		statusSender,
		config.MaxBlocksPerRound,
		config.MaxBlocksPerColdCall,
	}
}

func doneWaiter(wg *sync.WaitGroup, c <-chan error) {
	defer wg.Done()
	for {
		value, ok := <-c
		if !ok {
			fmt.Printf("Worker done\n")
			return
		}
		fmt.Printf("Worker received %v\n", value)
	}
}

func (sr *SinkResponder[I, R]) AllDone() {
	var wg sync.WaitGroup

	for _, sessionKey := range sr.sinkSessions.Keys() {
		session, ok := sr.sinkSessions.Get(sessionKey)
		if !ok {
			log.Warnw("Done: session not found", "sessionKey", sessionKey)
			continue
		}
		wg.Add(1)
		go doneWaiter(&wg, session.Session.Done())
	}

	wg.Wait()
}

func (sr *SinkResponder[I, R]) newSinkConnection() *GenericBatchSinkConnection[I, R] {
	return NewGenericBatchSinkConnection[I, R](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE, sr.maxBlocksPerRound, false) // Responders aren't requesters
}

func (sr *SinkResponder[I, R]) SinkSessionData(sessionId SessionId) *SinkSessionData[I, R] {
	sessionData := sr.sinkSessions.GetOrInsert(
		sessionId,
		func() *SinkSessionData[I, R] {
			return sr.startSinkSession(sessionId)
		},
	)

	log.Debugw("SinkSessionData", "sessionId", sessionId, "sessionData", sessionData)

	return sessionData
}

func (sr *SinkResponder[I, R]) SinkSession(sessionId SessionId) *core.SinkSession[I, BatchState] {
	sessionData := sr.SinkSessionData(sessionId)
	return sessionData.Session
}

// func (sr *SinkResponder[I, R]) SinkSessions() []core.SinkSession[I, BatchState] {
// 	return sr.sinkSessions.Keys()
// }

func (sr *SinkResponder[I, R]) SinkSessionIds() []SessionId {
	return sr.sinkSessions.Keys()
}

func (sr *SinkResponder[I, R]) SinkConnection(sessionId SessionId) *GenericBatchSinkConnection[I, R] {
	sessionData := sr.SinkSessionData(sessionId)
	return sessionData.conn
}

func (sr *SinkResponder[I, R]) Receiver(sessionId SessionId) *SimpleBatchBlockReceiver[I] {
	sessionData := sr.SinkSessionData(sessionId)
	return sessionData.conn.Receiver(sessionData.Session)
}

func (sr *SinkResponder[I, R]) startSinkSession(sessionId SessionId) *SinkSessionData[I, R] {

	sinkConnection := sr.newSinkConnection()

	newSession := sinkConnection.Session(
		sr.store,
		// Since SimpleStatusAccumulator uses mutex locks, the filter allocator just returns a non-sync filter
		core.NewSimpleStatusAccumulator(sr.filterAllocator()),
		false, // Not a requester
	)

	// TODO: validate that we have a batch status sender first

	// This is nil, as expected since we didn't call SetStatusSender
	// This isn't respecting setStatusSender now
	// Total hack currently
	var statusSender core.StatusSender[I]
	if sr.statusSender == nil {
		statusSender = sinkConnection.Sender(sinkConnection.DeferredSender())
	} else {
		// This is just for test currently.  Remove it.
		statusSender = sinkConnection.Sender(sr.statusSender)
	}

	go func() {
		newSession.Run(statusSender)
		// statusSender.Close()
		sr.sinkSessions.Remove(sessionId)
	}()

	// Wait for the session to start
	<-newSession.Started()

	return &SinkSessionData[I, R]{
		sinkConnection,
		newSession,
	}
}

func (sr *SinkResponder[I, R]) SetStatusSender(statusSender core.StatusSender[I]) {
	sr.statusSender = statusSender
}

// Need to get sink session, start it, allow dynamic looking up of its receiver

type Config struct {
	MaxBlocksPerRound    uint32
	MaxBlocksPerColdCall uint32
	BloomCapacity        uint
	BloomFunction        uint64
	Instrument           instrumented.InstrumentationOptions
}

func DefaultConfig() Config {
	return Config{
		MaxBlocksPerRound:    100,
		MaxBlocksPerColdCall: 10,
		BloomCapacity:        1024,
		BloomFunction:        2,
		Instrument:           0,
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

type SourceSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	// conn is different for http.  Is a func the way to go or generics?
	// Just using what we need in test to start with.
	conn    *GenericBatchSourceConnection[I, R]
	Session *core.SourceSession[I, BatchState]
}

type SourceResponder[I core.BlockId, R core.BlockIdRef[I]] struct {
	// for responder sessions, we don't need to do anything to them other then Run?  No, we might need to cancel.
	store          core.BlockStore[I]
	sourceSessions *util.SynchronizedMap[SessionId, *SourceSessionData[I, R]]
	// TODO: SourceSession does not sync wrapping of the filter, ...
	filterAllocator      func() filter.Filter[I]
	batchBlockSender     BatchBlockSender[I]
	maxBlocksPerRound    uint32
	maxBlocksPerColdCall uint32
}

func NewSourceResponder[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config, batchBlockSender BatchBlockSender[I]) *SourceResponder[I, R] {
	return &SourceResponder[I, R]{
		store,
		util.NewSynchronizedMap[SessionId, *SourceSessionData[I, R]](),
		NewBloomAllocator[I](&config),
		batchBlockSender,
		config.MaxBlocksPerRound,
		config.MaxBlocksPerColdCall,
	}
}

// func doneWaiter(wg *sync.WaitGroup, c <-chan error) {
// 	defer wg.Done()
// 	for {
// 		value, ok := <-c
// 		if !ok {
// 			fmt.Printf("Worker done\n")
// 			return
// 		}
// 		fmt.Printf("Worker received %v\n", value)
// 	}
// }

func (sr *SourceResponder[I, R]) AllDone() {
	var wg sync.WaitGroup

	for _, sessionKey := range sr.sourceSessions.Keys() {
		session, ok := sr.sourceSessions.Get(sessionKey)
		if !ok {
			log.Warnw("Done: session not found", "sessionKey", sessionKey)
			continue
		}
		wg.Add(1)
		go doneWaiter(&wg, session.Session.Done())
	}

	wg.Wait()
}

func (sr *SourceResponder[I, R]) newSourceConnection() *GenericBatchSourceConnection[I, R] {
	return NewGenericBatchSourceConnection[I, R](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE, sr.maxBlocksPerRound, false) // Responders aren't requesters
}

func (sr *SourceResponder[I, R]) SourceSessionData(sessionId SessionId) *SourceSessionData[I, R] {
	sessionData := sr.sourceSessions.GetOrInsert(
		sessionId,
		func() *SourceSessionData[I, R] {
			return sr.startSourceSession(sessionId)
		},
	)
	log.Debugw("SourceSessionData", "sessionId", sessionId, "sessionData", sessionData)

	return sessionData
}

func (sr *SourceResponder[I, R]) SourceSession(sessionId SessionId) *core.SourceSession[I, BatchState] {
	sessionData := sr.SourceSessionData(sessionId)
	return sessionData.Session
}

func (sr *SourceResponder[I, R]) SourceSessionIds() []SessionId {
	return sr.sourceSessions.Keys()
}

func (sr *SourceResponder[I, R]) SourceConnection(sessionId SessionId) *GenericBatchSourceConnection[I, R] {
	sessionData := sr.SourceSessionData(sessionId)
	return sessionData.conn
}

func (sr *SourceResponder[I, R]) Receiver(sessionId SessionId) *SimpleBatchStatusReceiver[I] {
	sessionData := sr.SourceSessionData(sessionId)
	return sessionData.conn.Receiver(sessionData.Session)
}

func (sr *SourceResponder[I, R]) startSourceSession(sessionId SessionId) *SourceSessionData[I, R] {

	sourceConnection := sr.newSourceConnection()

	newSession := sourceConnection.Session(
		sr.store,
		// TODO: SourceSession has no mutex locks for filter, so we need to wrap.  Inconsistent though and really should be cleaned up.
		filter.NewSynchronizedFilter(sr.filterAllocator()),
		false, // Not a requester
	)

	var batchBlockSender core.BlockSender[I]
	if sr.batchBlockSender == nil {
		batchBlockSender = sourceConnection.Sender(sourceConnection.DeferredBatchSender(), sr.maxBlocksPerRound)
	} else {
		// This is just for test currently.  Remove it.
		batchBlockSender = sourceConnection.Sender(sr.batchBlockSender, sr.maxBlocksPerRound)
	}

	go func() {
		newSession.Run(batchBlockSender)
		// batchBlockSender.Close()
		sr.sourceSessions.Remove(sessionId)
	}()

	// Wait for the session to start
	<-newSession.Started()

	return &SourceSessionData[I, R]{
		sourceConnection,
		newSession,
	}
}

func (sr *SourceResponder[I, R]) SetBatchBlockSender(batchBlockSender BatchBlockSender[I]) {
	sr.batchBlockSender = batchBlockSender
}
