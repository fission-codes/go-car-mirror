package carmirror

import (
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"

	. "github.com/fission-codes/go-car-mirror/filter"
)

var log *zap.SugaredLogger

func init() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	log = logger.Sugar()
}

// BlockId represents a unique identifier for a Block.
// This interface only represents the identifier, not the Block.
type BlockId comparable

// Block is an immutable data block referenced by a unique ID.
type Block[I BlockId] interface {
	// TODO: Should I add an iterator type here?

	// Id returns the BlockId for the Block.
	Id() I

	// Children returns a list of `BlockId`s linked to from the Block.
	Children() []I
}

// ReadableBlockStore represents read operations for a store of blocks.
type ReadableBlockStore[I BlockId] interface {
	// Get gets the block from the blockstore with the given ID.
	Get(I) (Block[I], error)

	// Has returns true if the blockstore has a block with the given ID.
	Has(I) (bool, error)

	// All returns a channel that will receive all of the block IDs in this store.
	All() (<-chan I, error)
}

type BlockStore[I BlockId] interface {
	ReadableBlockStore[I]

	// Add puts a given block to the blockstore.
	Add(Block[I]) error
}

type SynchronizedBlockStore[I BlockId] struct {
	store BlockStore[I]
	mutex sync.RWMutex
}

func NewSynchronizedBlockStore[I BlockId](store BlockStore[I]) *SynchronizedBlockStore[I] {
	return &SynchronizedBlockStore[I]{store, sync.RWMutex{}}
}

func (bs *SynchronizedBlockStore[I]) Get(id I) (Block[I], error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	return bs.store.Get(id)
}

func (bs *SynchronizedBlockStore[I]) Has(id I) (bool, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	return bs.store.Has(id)
}

func (bs *SynchronizedBlockStore[I]) All() (<-chan I, error) {
	bs.mutex.RLock()
	if all, err := bs.store.All(); err == nil {
		res := make(chan I)
		go func() {
			defer bs.mutex.RUnlock()
			for id := range all {
				res <- id
			}
		}()
		return res, nil
	} else {
		bs.mutex.RUnlock()
		return nil, err
	}
}

func (bs *SynchronizedBlockStore[I]) Add(block Block[I]) error {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	return bs.store.Add(block)

}

type MutablePointerResolver[I BlockId] interface {
	// Resolve attempts to resolve ptr into a block ID.
	Resolve(ptr string) (I, error)
}

// BlockSender is responsible for sending blocks - immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId] interface {
	SendBlock(block Block[I]) error
	Flush() error
	Close() error
}

// BlockReceiver is responsible for receiving blocks.
type StateReceiver[F Flags] interface {
	// HandleBlock is called on receipt of a new block.
	HandleState(state F)
}

// BlockReceiver is responsible for receiving blocks.
type BlockReceiver[I BlockId, F Flags] interface {
	StateReceiver[F]
	// HandleBlock is called on receipt of a new block.
	HandleBlock(block Block[I])
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// The StatusSender will therefore usually batch reported information and send it in bulk to the ReceiverSession.
type StatusSender[I BlockId] interface {
	SendStatus(have Filter[I], want []I) error
	Close() error
}

// StatusReceiver is responsible for receiving a status.
type StatusReceiver[I BlockId, F Flags] interface {
	StateReceiver[F]
	HandleStatus(have Filter[I], want []I)
}

// StatusAccumulator is responsible for collecting status.
type StatusAccumulator[I BlockId] interface {
	Have(I) error
	Want(I) error
	Send(StatusSender[I]) error
	Receive(I) error
}

type SessionEvent uint16

const (
	BEGIN_SESSION SessionEvent = iota
	END_SESSION
	BEGIN_DRAINING
	END_DRAINING
	BEGIN_CLOSE
	END_CLOSE
	BEGIN_FLUSH
	END_FLUSH
	BEGIN_SEND
	END_SEND
	BEGIN_RECEIVE
	END_RECEIVE
	BEGIN_CHECK
	END_CHECK
)

func (se SessionEvent) String() string {
	switch se {
	case BEGIN_SESSION:
		return "BEGIN_SESSION"
	case END_SESSION:
		return "END_SESSION"
	case BEGIN_DRAINING:
		return "BEGIN_DRAINING"
	case END_DRAINING:
		return "END_DRAINING"
	case BEGIN_CLOSE:
		return "BEGIN_CLOSE"
	case END_CLOSE:
		return "END_CLOSE"
	case BEGIN_FLUSH:
		return "BEGIN_FLUSH"
	case END_FLUSH:
		return "END_FLUSH"
	case BEGIN_SEND:
		return "BEGIN_SEND"
	case END_SEND:
		return "END_SEND"
	case BEGIN_RECEIVE:
		return "BEGIN_RECEIVE"
	case END_RECEIVE:
		return "END_RECEIVE"
	case BEGIN_CHECK:
		return "BEGIN_CHECK"
	case END_CHECK:
		return "END_CHECK"
	default:
		return "<unknown>"
	}
}

// Internal state...
type Flags constraints.Unsigned

// Orchestrator is responsible for managing the flow of blocks and/or status.
type Orchestrator[F Flags] interface {
	Notify(SessionEvent) error

	// GetState is used to obtain state to send to a remote session.
	GetState() F

	// ReceiveState is used to receive state from a remote session.
	ReceiveState(F) error

	IsClosed() bool
}

type SenderConnection[
	F Flags,
	I BlockId,
] interface {
	GetBlockSender(Orchestrator[F]) BlockSender[I]
}

type ReceiverConnection[
	F Flags,
	I BlockId,
] interface {
	GetStatusSender(Orchestrator[F]) StatusSender[I]
}

type ReceiverSession[
	I BlockId,
	F Flags,
] struct {
	accumulator  StatusAccumulator[I]
	connection   ReceiverConnection[F, I]
	orchestrator Orchestrator[F]
	store        BlockStore[I]
	pending      chan Block[I]
	// pending      map[I]bool
	// pendingMutex sync.RWMutex
}

func NewReceiverSession[I BlockId, F Flags](
	store BlockStore[I],
	connection ReceiverConnection[F, I],
	accumulator StatusAccumulator[I],
	orchestrator Orchestrator[F],
) *ReceiverSession[I, F] {
	return &ReceiverSession[I, F]{
		accumulator,
		connection,
		orchestrator,
		store,
		make(chan Block[I], 1024),
	}
}

func (rs *ReceiverSession[I, F]) AccumulateStatus(id I) error {
	// Get block and handle errors
	block, err := rs.store.Get(id)

	if err == errors.BlockNotFound {
		return rs.accumulator.Want(id)
	}

	if err != nil {
		return err
	}

	if err := rs.accumulator.Have(id); err != nil {
		return err
	}

	for _, child := range block.Children() {
		if err := rs.AccumulateStatus(child); err != nil {
			return err
		}
	}

	return nil
}

func (rs *ReceiverSession[I, F]) HandleBlock(block Block[I]) {
	rs.orchestrator.Notify(BEGIN_RECEIVE)
	defer rs.orchestrator.Notify(END_RECEIVE)

	if err := rs.store.Add(block); err != nil {
		log.Debugf("Failed to add block to store. err = %v", err)
	}

	if err := rs.accumulator.Receive(block.Id()); err != nil {
		log.Debugf("Failed to receive block. err = %v", err)
	}

	rs.pending <- block
}

func (rs *ReceiverSession[I, F]) Run() error {
	sender := rs.connection.GetStatusSender(rs.orchestrator)

	rs.orchestrator.Notify(BEGIN_SESSION)
	defer func() {
		rs.orchestrator.Notify(END_SESSION)
		sender.Close()
	}()

	for !rs.orchestrator.IsClosed() {
		// TODO: Look into use of defer for this.
		rs.orchestrator.Notify(BEGIN_CHECK)

		// TODO: Any concerns with hangs here if nothing in pending?  Need goroutines?
		if len(rs.pending) > 0 {
			block := <-rs.pending

			for _, child := range block.Children() {
				if err := rs.AccumulateStatus(child); err != nil {
					return err
				}
			}
		} else {
			// If we get here it means we have no more blocks to process. In a batch based process that's
			// the only time we'd want to send. But for streaming maybe this should be in a separate loop
			// so it can be triggered by the orchestrator - otherwise we wind up sending a status update every
			// time the pending list becomes empty.

			rs.orchestrator.Notify(BEGIN_SEND)
			rs.accumulator.Send(sender)
			rs.orchestrator.Notify(END_SEND)

		}
		rs.orchestrator.Notify(END_CHECK)
	}

	return nil
}

func (ss *ReceiverSession[I, F]) HandleState(state F) {
	err := ss.orchestrator.ReceiveState(state)
	if err != nil {
		log.Errorw("ReceiverSession", "method", "HandleState", "error", err)
	}
}

type SenderSession[
	I BlockId,
	F Flags,
] struct {
	store        BlockStore[I]
	connection   SenderConnection[F, I]
	orchestrator Orchestrator[F]
	filter       Filter[I]
	sent         sync.Map
	pending      chan I
}

func NewSenderSession[I BlockId, F Flags](store BlockStore[I], connection SenderConnection[F, I], filter Filter[I], orchestrator Orchestrator[F]) *SenderSession[I, F] {
	return &SenderSession[I, F]{
		store,
		connection,
		orchestrator,
		filter,
		sync.Map{},
		make(chan I, 1024),
	}
}

func (ss *SenderSession[I, F]) Run() error {
	sender := ss.connection.GetBlockSender(ss.orchestrator)
	if err := ss.orchestrator.Notify(BEGIN_SESSION); err != nil {
		return err
	}

	for !ss.orchestrator.IsClosed() {

		if err := ss.orchestrator.Notify(BEGIN_SEND); err != nil {
			return err
		}

		if len(ss.pending) > 0 {
			id := <-ss.pending
			if _, ok := ss.sent.Load(id); !ok {
				ss.sent.Store(id, true)

				block, err := ss.store.Get(id)
				if err != nil {
					if err != errors.BlockNotFound {
						return err
					}
				} else {
					if err := sender.SendBlock(block); err != nil {
						return err
					}
					for _, child := range block.Children() {
						if ss.filter.DoesNotContain(child) {
							ss.pending <- child
						}
					}
				}
			}
		} else {
			if err := ss.orchestrator.Notify(BEGIN_DRAINING); err != nil {
				return err
			}
			if err := sender.Flush(); err != nil {
				return err
			}
			ss.orchestrator.Notify(END_DRAINING)
		}
		ss.orchestrator.Notify(END_SEND)
	}
	ss.orchestrator.Notify(END_SESSION)
	if err := sender.Close(); err != nil {
		return err
	}
	return nil
}

func (ss *SenderSession[I, F]) HandleStatus(have Filter[I], want []I) {
	if err := ss.orchestrator.Notify(BEGIN_RECEIVE); err != nil {
		log.Errorw("SenderSession", "method", "HandleStatus", "error", err)
	}
	defer ss.orchestrator.Notify(END_RECEIVE)

	ss.filter = ss.filter.AddAll(have)

	for _, id := range want {
		ss.pending <- id
	}
}

func (ss *SenderSession[I, F]) Close() error {
	if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	defer ss.orchestrator.Notify(END_CLOSE)

	// TODO: Clear the bloom filter

	return nil
}

func (ss *SenderSession[I, F]) Enqueue(id I) error {
	ss.pending <- id

	return nil
}

func (ss *SenderSession[I, F]) HandleState(state F) {
	err := ss.orchestrator.ReceiveState(state)
	if err != nil {
		log.Errorw("SenderSession", "method", "HandleState", "error", err)
	}
}
