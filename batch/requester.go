package batch

// import (
// 	"errors"

// 	"github.com/fission-codes/go-car-mirror/core"
// 	"github.com/fission-codes/go-car-mirror/core/instrumented"
// 	"github.com/fission-codes/go-car-mirror/filter"
// 	"github.com/fission-codes/go-car-mirror/stats"
// 	"github.com/fission-codes/go-car-mirror/util"
// )

// func init() {
// 	stats.InitDefault()
// }

// var (
// 	ErrInvalidSession = errors.New("invalid session")
// )

// type blockSenderGetterFunc[I core.BlockId] func(sourceSession *core.SourceSession[I, BatchState], maxBatchSize uint32) core.BlockSender[I]
// type filterAllocaterFunc[I core.BlockId] func() filter.Filter[I]

// type SourceRequester[I core.BlockId] struct {
// 	store           core.BlockStore[I]
// 	sourceSessions  *util.SynchronizedMap[string, *core.SourceSession[I, BatchState]]
// 	maxBatchSize    uint32
// 	filterAllocater filterAllocaterFunc[I]
// 	instrumented    instrumented.InstrumentationOptions
// 	senderGetter    blockSenderGetterFunc[I]
// }

// func NewSourceRequester[I core.BlockId](store core.BlockStore[I], config Config, senderGetter blockSenderGetterFunc[I]) *SourceRequester[I] {
// 	return &SourceRequester[I]{
// 		store,
// 		util.NewSynchronizedMap[string, *core.SourceSession[I, BatchState]](),
// 		config.MaxBatchSize,
// 		NewBloomAllocator[I](&config),
// 		config.Instrument,
// 		senderGetter,
// 	}
// }

// func (r *SourceRequester[I]) SourceConnection() *GenericBatchSourceConnection[I] {
// 	return NewGenericBatchSourceConnection[I](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)
// }

// // Sender returns the sender for the session with the specified session ID.
// func (r *SourceRequester[I]) Sender(sessionId string, core.BlockSender[I]) core.BlockSender[I] {
// 	return r.SourceConnection().Sender()
// }

// func (r *SourceRequester[I]) SourceSession(sessionId string) *core.SourceSession[I, BatchState] {
// 	return r.sourceSessions.GetOrInsert(
// 		sessionId,
// 		func() *core.SourceSession[I, BatchState] {
// 			return r.SourceConnection().Session(
// 				r.store,
// 				filter.NewSynchronizedFilter[I](filter.NewEmptyFilter(r.filterAllocater)),
// 			)
// 		},
// 	)
// }

// // TODO: synchronize starting of sessions
// func (r *SourceRequester[I]) StartSourceSession(sessionId string) *core.SourceSession[I, BatchState] {
// 	sourceSession := r.SourceSession(sessionId)
// 	blockSender := r.Sender(sessionId)

// 	go func() {
// 		log.Debugw("starting source session", "object", "SourceRequester", "method", "startSourceSession", "sessionId", sessionId)
// 		sourceSession.Run(blockSender)

// 		// TODO: potential race condition if Run() completes before the
// 		// session is added to the list of sink sessions (which happens
// 		// when startSourceSession returns)
// 		r.sourceSessions.Remove(sessionId)
// 		log.Debugw("source session ended", "object", "SourceRequester", "method", "startSourceSession", "sessionId", sessionId)
// 	}()

// 	// Wait for the session to start
// 	// TODO: Is this even needed or desireable?  I added while debugging other things.
// 	<-sourceSession.Started()

// 	return sourceSession
// }

// func (r *SourceRequester[I]) SourceSessions() []string {
// 	return r.sourceSessions.Keys()
// }

// func (r *SourceRequester[I]) SourceInfo(sessionId string) (*core.SourceSessionInfo[BatchState], error) {
// 	if session, ok := r.sourceSessions.Get(sessionId); ok {
// 		return session.Info(), nil
// 	} else {
// 		return nil, ErrInvalidSession
// 	}
// }

// // TODO: This is only used in test now.  Remove?
// func (r *SourceRequester[I]) Send(sessionId string, id I) error {
// 	session := r.SourceSession(sessionId)
// 	return session.Enqueue(id)
// }

// // Cancel cancels the source session with the given URL.
// func (r *SourceRequester[I]) Cancel(sessionId string) error {
// 	log.Debugw("enter", "object", "SourceRequester", "method", "Cancel", "sessionId", sessionId)
// 	session := r.SourceSession(sessionId)
// 	err := session.Cancel()
// 	log.Debugw("exit", "object", "SourceRequester", "method", "Cancel", "err", err)
// 	return err
// }

// type Config struct {
// 	MaxBatchSize  uint32
// 	BloomCapacity uint
// 	BloomFunction uint64
// 	Instrument    instrumented.InstrumentationOptions
// }

// func DefaultConfig() Config {
// 	return Config{
// 		MaxBatchSize:  32,
// 		BloomCapacity: 1024,
// 		BloomFunction: 2,
// 		Instrument:    0,
// 	}
// }

// func NewBloomAllocator[I core.BlockId](config *Config) func() filter.Filter[I] {
// 	return func() filter.Filter[I] {
// 		filter, err := filter.TryNewBloomFilter[I](config.BloomCapacity, config.BloomFunction)
// 		if err != nil {
// 			log.Errorf("Invalid hash function specified %v", config.BloomFunction)
// 		}
// 		return filter
// 	}
// }
