// Package carmirror provides a generic Go implementation of the [CAR Mirror] protocol.
//
// [CAR Mirror]: https://github.com/fission-codes/spec/blob/main/car-pool/car-mirror/SPEC.md
package carmirror

import (
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"

	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("go-car-mirror")

// BlockId represents a unique identifier for a Block.
// This interface only represents the identifier, not the Block. The interface is chosen for compatibility
// with ipfs/go-cid - noting that go-cid is, for the moment, comparable.
//
// It's annoying that go-cid doesn't implement cbor.Marshaler/cbor.Unmarshaler because the binary
// representation of a CID and the CBOR canonical representation are quite different:
//   - [https://pkg.go.dev/github.com/ipfs/go-cid#Cast] - binary form of CID
//   - [https://ipld.io/specs/codecs/dag-cbor/spec/#links] - CBOR representation
//
// Unfortunately, we intend to leverage the CAR v1 spec for compatibility, which, oddly, specifies
// using the CBOR form of the CID in the header and the raw byte form in the body. Thus, our interface here
// needs to be able support both.
type BlockId interface {
	comparable
	encoding.BinaryMarshaler
	json.Marshaler
	cbor.Marshaler
	String() string
}

// BlockIdRef is a reference to a BlockId.
// In Go, you are not supposed to have a mix of pointer and non-pointer receivers for the same interface.
// go-cid has a mix of pointer and non-pointer receivers.
// Since unmarshaling a BlockId will require a pointer receiver in order to update the BlockId, we needed a separate interface.
type BlockIdRef[T BlockId] interface {
	*T
	encoding.BinaryUnmarshaler
	json.Unmarshaler
	cbor.Unmarshaler
	Read(ByteAndBlockReader) (int, error)
}

// RawBlock represents a raw block before any association with a blockstore.
type RawBlock[I BlockId] interface {
	// Id returns the BlockId for the Block.
	Id() I
	// RawData returns the raw data bytes for the Block.
	RawData() []byte
	// Size returns the size of the block
	Size() int64
}

// BlockEqual returns true if the two blocks are equal.
func BlockEqual[I BlockId](a RawBlock[I], b RawBlock[I]) bool {
	return a.Id() == b.Id() && slices.Equal(a.RawData(), b.RawData())
}

// Block is an immutable data block referenced by a unique ID. It may reference other blocks by Id.
type Block[I BlockId] interface {
	RawBlock[I]

	// Children returns the BlockIds of the children of this block.
	Children() []I
}

// ReadableBlockStore represents read operations for a store of blocks.
type ReadableBlockStore[I BlockId] interface {
	// Get returns the block from the blockstore with the given ID.
	Get(context.Context, I) (Block[I], error)

	// Has returns true if the blockstore has a block with the given ID.
	Has(context.Context, I) (bool, error)

	// All returns a channel that will receive all of the block IDs in this store.
	All(context.Context) (<-chan I, error)
}

// BlockStore represents read and write operations for a store of blocks.
type BlockStore[I BlockId] interface {
	ReadableBlockStore[I]

	// Add puts a given block to the blockstore.
	Add(context.Context, RawBlock[I]) (Block[I], error)
}

// SynchronizedBlockStore is a BlockStore that is also thread-safe.
type SynchronizedBlockStore[I BlockId] struct {
	store BlockStore[I]
	mutex sync.RWMutex
}

// NewSynchronizedBlockStore creates a new SynchronizedBlockStore.
func NewSynchronizedBlockStore[I BlockId](store BlockStore[I]) *SynchronizedBlockStore[I] {
	return &SynchronizedBlockStore[I]{store, sync.RWMutex{}}
}

// Get returns the block from the synchronized blockstore with the given ID.
func (bs *SynchronizedBlockStore[I]) Get(ctx context.Context, id I) (Block[I], error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	return bs.store.Get(ctx, id)
}

// Has returns true if the synchronized blockstore has a block with the given ID.
func (bs *SynchronizedBlockStore[I]) Has(ctx context.Context, id I) (bool, error) {
	bs.mutex.RLock()
	defer bs.mutex.RUnlock()
	return bs.store.Has(ctx, id)
}

// All returns a channel that will receive all of the block IDs in this synchronized blockstore.
func (bs *SynchronizedBlockStore[I]) All(ctx context.Context) (<-chan I, error) {
	bs.mutex.RLock()
	if all, err := bs.store.All(ctx); err == nil {
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

// Add adds a given RawBlock to the synchronized blockstore, returnng the Block.
func (bs *SynchronizedBlockStore[I]) Add(ctx context.Context, block RawBlock[I]) (Block[I], error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	return bs.store.Add(ctx, block)
}

// MutablePointerResolver is responsible for resolving a pointer into a BlockId.
type MutablePointerResolver[I BlockId] interface {
	// Resolve attempts to resolve ptr into a block ID.
	Resolve(ptr string) (I, error)
}

// BlockSender is responsible for sending blocks, immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId] interface {
	SendBlock(block RawBlock[I]) error
	Flush() error
	Close() error
}

// BlockReceiver is responsible for receiving blocks.
type StateReceiver[F Flags] interface {
	// HandleState is called on receipt of a new state.
	HandleState(state F)
}

// BlockReceiver is responsible for receiving blocks.
type BlockReceiver[I BlockId, F Flags] interface {
	StateReceiver[F]
	// HandleBlock is called on receipt of a new block.
	HandleBlock(block RawBlock[I])
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// TODO: this use of status implies it's just filter, not want list.  Be precise in language.
// The StatusSender will therefore usually batch reported information and send it in bulk to the ReceiverSession.
type StatusSender[I BlockId] interface {
	// SendStatus sends the status to the ReceiverSession.
	// The have filter is a lossy filter of the blocks that the SenderSession has.
	// The want list is a list of blocks that the SenderSession wants.
	SendStatus(have filter.Filter[I], want []I) error

	// TODO: Close closes the ???.
	Close() error
}

// StatusReceiver is responsible for receiving a status.
// It also receives the state of the session.
type StatusReceiver[I BlockId, F Flags] interface {
	StateReceiver[F]

	// HandleStatus is called on receipt of a new status.
	// The have filter is a lossy filter of the blocks that the ??? has.
	// The want list is a list of blocks that the ??? wants.
	HandleStatus(have filter.Filter[I], want []I)
}

// StatusAccumulator is responsible for collecting status.
type StatusAccumulator[I BlockId] interface {
	// Have records that the Sink has a block.
	Have(I) error
	// Get the number of unique Haves since the last Send
	HaveCount() uint
	// Want records that the Sink wants a block.
	Want(I) error
	// Get the number of unique Wants since the last Send
	WantCount() uint

	// Send sends the status to the StatusSender.
	Send(StatusSender[I]) error

	// Receive records that the block id has been received, updating any accumulated status as a result.
	Receive(I) error
}

// SessionEvent is an event that can occur during a session.
// When events occur, they trigger updates to session state.
type SessionEvent uint16

// Core session event constants.
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
	CANCEL
)

// String returns a string representation of the session event.
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
	case CANCEL:
		return "CANCEL"
	default:
		return "<unknown>"
	}
}

// Flags represent the internal state of a session.
type Flags constraints.Unsigned

// Orchestrator is responsible for managing the flow of blocks and/or status.
type Orchestrator[F Flags] interface {
	// Notify is used to notify the orchestrator of an event.
	Notify(SessionEvent) error

	// State is used to obtain state to send to a remote session.
	State() F

	// ReceiveState is used to receive state from a remote session.
	ReceiveState(F) error

	// IsClosed returns true if the orchestrator is closed.
	IsClosed() bool
}

// SenderConnection provides a way to get a block sender after you create the session.
// The sender wants to know about the orchestrator, to notify it of certain events, like flush.
type SenderConnection[
	F Flags,
	I BlockId,
] interface {
	// OpenBlockSender opens a block sender.
	OpenBlockSender(Orchestrator[F]) BlockSender[I]
}

// ReceiverConnection provides a way to get a status sender after you create the session.
type ReceiverConnection[
	F Flags,
	I BlockId,
] interface {
	// OpenStatusSender opens a status sender.
	OpenStatusSender(Orchestrator[F]) StatusSender[I]
}

// ReceiverSession is a session that receives blocks.
type ReceiverSession[
	I BlockId,
	F Flags,
] struct {
	accumulator  StatusAccumulator[I]
	orchestrator Orchestrator[F]
	store        BlockStore[I]
	pending      *util.SynchronizedDeque[Block[I]]
	log          *zap.SugaredLogger
}

// Struct for returning summary information about the session
type ReceiverSessionInfo[F Flags] struct {
	PendingCount  uint
	Status        F
	HavesEstimate uint
	Wants         uint
}

func (inf *ReceiverSessionInfo[F]) String() string {
	return fmt.Sprintf("pnd:%6v have:%6v want:%6v %v", inf.PendingCount, inf.HavesEstimate, inf.Wants, inf.Status)
}

// NewReceiverSession creates a new ReceiverSession.
func NewReceiverSession[I BlockId, F Flags](
	store BlockStore[I],
	accumulator StatusAccumulator[I],
	orchestrator Orchestrator[F],
) *ReceiverSession[I, F] {
	return &ReceiverSession[I, F]{
		accumulator,
		orchestrator,
		store,
		util.NewSynchronizedDeque[Block[I]](util.NewBlocksDeque[Block[I]](2048)),
		&log.SugaredLogger,
	}
}

func (rs *ReceiverSession[I, F]) GetInfo() *ReceiverSessionInfo[F] {
	return &ReceiverSessionInfo[F]{
		PendingCount:  uint(rs.pending.Len()),
		Status:        rs.orchestrator.State(),
		HavesEstimate: rs.accumulator.HaveCount(),
		Wants:         rs.accumulator.WantCount(),
	}
}

// AccumulateStatus accumulates the status of the block with the given id and all of its children.
func (rs *ReceiverSession[I, F]) AccumulateStatus(id I) error {
	// Get block and handle errors
	block, err := rs.store.Get(context.Background(), id)

	if err == errors.ErrBlockNotFound {
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

// HandleBlock handles a block that is being received.
func (rs *ReceiverSession[I, F]) HandleBlock(rawBlock RawBlock[I]) {
	rs.orchestrator.Notify(BEGIN_RECEIVE)
	defer rs.orchestrator.Notify(END_RECEIVE)

	block, err := rs.store.Add(context.TODO(), rawBlock)
	if err != nil {
		rs.log.Debugf("Failed to add block to store. err = %v", err)
	}

	if err := rs.accumulator.Receive(block.Id()); err != nil {
		rs.log.Debugf("Failed to receive block. err = %v", err)
	}

	rs.pending.PushBack(block)
}

// Run runs the receiver session.
// TODO: Is start a better name?  Starting a session?
// Or begin, to match the event names?
func (rs *ReceiverSession[I, F]) Run(connection ReceiverConnection[F, I]) error {
	sender := connection.OpenStatusSender(rs.orchestrator)

	rs.orchestrator.Notify(BEGIN_SESSION)
	defer func() {
		rs.orchestrator.Notify(END_SESSION)
		sender.Close()
	}()

	for !rs.orchestrator.IsClosed() {
		rs.orchestrator.Notify(BEGIN_CHECK)

		if rs.pending.Len() > 0 {
			block := rs.pending.PollFront()

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

// HandleState makes sure the state is handled by the orchestrator.
func (ss *ReceiverSession[I, F]) HandleState(state F) {
	err := ss.orchestrator.ReceiveState(state)
	if err != nil {
		ss.log.Errorw("receiving state", "object", "ReceiverSession", "method", "HandleState", "error", err)
	}
}

// Close closes the sender session.
func (rs *ReceiverSession[I, F]) Close() error {
	if err := rs.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	defer rs.orchestrator.Notify(END_CLOSE)

	return nil
}

// Cancel cancels the session.
func (ss *ReceiverSession[I, F]) Cancel() error {
	return ss.orchestrator.Notify(CANCEL)
}

// IsClosed returns true if the session is closed.
func (rs *ReceiverSession[I, F]) IsClosed() bool {
	return rs.orchestrator.IsClosed()
}

// Struct for returting summary information about the session
type SenderSessionInfo[F Flags] struct {
	PendingCount  uint
	Status        F
	HavesEstimate uint
}

func (inf *SenderSessionInfo[F]) String() string {
	return fmt.Sprintf("pnd:%6v have:%6v %v", inf.PendingCount, inf.HavesEstimate, inf.Status)
}

// SenderSession is a session for sending blocks.
type SenderSession[
	I BlockId,
	F Flags,
] struct {
	store        BlockStore[I]
	orchestrator Orchestrator[F]
	filter       filter.Filter[I]
	sent         sync.Map
	pending      *util.SynchronizedDeque[I]
	log          *zap.SugaredLogger
}

// NewSenderSession creates a new SenderSession.
func NewSenderSession[I BlockId, F Flags](store BlockStore[I], filter filter.Filter[I], orchestrator Orchestrator[F]) *SenderSession[I, F] {
	return &SenderSession[I, F]{
		store,
		orchestrator,
		filter,
		sync.Map{},
		util.NewSynchronizedDeque[I](util.NewBlocksDeque[I](1024)),
		&log.SugaredLogger,
	}
}

func (ss *SenderSession[I, F]) GetInfo() *SenderSessionInfo[F] {
	return &SenderSessionInfo[F]{
		PendingCount:  uint(ss.pending.Len()),
		Status:        ss.orchestrator.State(),
		HavesEstimate: uint(ss.filter.Count()),
	}
}

// Run runs the sender session.
// TODO: Consider renaming to Start or Begin or Open to match Close/IsClosed?
func (ss *SenderSession[I, F]) Run(connection SenderConnection[F, I]) error {
	sender := connection.OpenBlockSender(ss.orchestrator)
	if err := ss.orchestrator.Notify(BEGIN_SESSION); err != nil {
		return err
	}

	for !ss.orchestrator.IsClosed() {

		if err := ss.orchestrator.Notify(BEGIN_SEND); err != nil {
			return err
		}

		if ss.pending.Len() > 0 {
			id := ss.pending.PollFront()
			if _, ok := ss.sent.Load(id); !ok {
				ss.sent.Store(id, true)

				block, err := ss.store.Get(context.Background(), id)
				if err != nil {
					if err != errors.ErrBlockNotFound {
						return err
					}
				} else {
					if err := sender.SendBlock(block); err != nil {
						return err
					}
					for _, child := range block.Children() {
						if ss.filter.DoesNotContain(child) {
							if err := ss.pending.PushBack(child); err != nil {
								return err
							}
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

// HandleStatus handles incoming status, updating the filter and pending list.
// TODO: Is pending just want but from the sender side?  and filter is have from the sender side?  Could we name these similarly?
// receiverHave, receiverWant?
func (ss *SenderSession[I, F]) HandleStatus(have filter.Filter[I], want []I) {
	if err := ss.orchestrator.Notify(BEGIN_RECEIVE); err != nil {
		ss.log.Errorw("error notifying BEGIN_RECEIVE", "object", "SenderSession", "method", "HandleStatus", "error", err)
	}
	defer ss.orchestrator.Notify(END_RECEIVE)
	ss.log.Debugw("begin processing", "object", "SenderSession", "method", "HandleStatus", "pending", ss.pending.Len(), "filter", ss.filter.Count())
	ss.filter = ss.filter.AddAll(have)
	ss.log.Debugw("incoming have filter merged", "object", "SenderSession", "method", "HandleStatus", "filter", ss.filter.Count())
	//ss.filter.Dump(ss.log, "SenderSession filter - ")
	for _, id := range want {
		ss.pending.PushFront(id) // send wants to the front of the queue
	}
	ss.log.Debugw("incoming want list merged", "obect", "SenderSession", "method", "HandleStatus", "pending", ss.pending.Len())
}

// Close closes the sender session.
func (ss *SenderSession[I, F]) Close() error {
	if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	defer ss.orchestrator.Notify(END_CLOSE)

	// TODO: Clear the bloom filter

	return nil
}

// Cancel cancels the sender session.
func (ss *SenderSession[I, F]) Cancel() error {
	return ss.orchestrator.Notify(CANCEL)
}

// Enqueue enqueues a block id to be sent.
func (ss *SenderSession[I, F]) Enqueue(id I) error {
	return ss.pending.PushBack(id)
}

// HandleState handles incoming session state.
func (ss *SenderSession[I, F]) HandleState(state F) {
	err := ss.orchestrator.ReceiveState(state)
	if err != nil {
		ss.log.Errorw("SenderSession", "method", "HandleState", "error", err)
	}
}

// IsClosed returns true if the session is closed.
func (ss *SenderSession[I, F]) IsClosed() bool {
	return ss.orchestrator.IsClosed()
}

// ByteAndBlockReader is an io.Reader that also implements io.ByteReader.
type ByteAndBlockReader interface {
	io.Reader
	io.ByteReader
}
