// Package carmirror provides a generic Go implementation of the [CAR Mirror] protocol.
//
// [CAR Mirror]: https://github.com/fission-codes/spec/blob/main/car-pool/car-mirror/SPEC.md
package core

import (
	"context"
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/stats"
	"github.com/fxamacker/cbor/v2"

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
type BlockReceiver[I BlockId, F Flags] interface {
	// HandleBlock is called on receipt of a new block.
	HandleBlock(block RawBlock[I])
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// TODO: this use of status implies it's just filter, not want list.  Be precise in language.
// The StatusSender will therefore usually batch reported information and send it in bulk to the SourceSession.
type StatusSender[I BlockId] interface {
	// SendStatus sends the status to the SourceSession.
	// The have filter is a lossy filter of the blocks that the SinkSession has.
	// The want list is a list of blocks that the SinkSession wants.
	SendStatus(have filter.Filter[I], want []I) error

	// TODO: Close closes the ???.
	Close() error
}

// StatusReceiver is responsible for receiving a status.
type StatusReceiver[I BlockId] interface {
	// HandleStatus is called on receipt of a new status.
	// The have filter is a lossy filter of the blocks that the Sink has.
	// The want list is a list of blocks that the Sink wants.
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
	BEGIN_BATCH
	END_BATCH
	BEGIN_ENQUEUE
	END_ENQUEUE
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
	case BEGIN_BATCH:
		return "BEGIN_BATCH"
	case END_BATCH:
		return "END_BATCH"
	case BEGIN_CHECK:
		return "BEGIN_CHECK"
	case END_CHECK:
		return "END_CHECK"
	case BEGIN_ENQUEUE:
		return "BEGIN_ENQUEUE"
	case END_ENQUEUE:
		return "END_ENQUEUE"
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

// SinkSession is a session that receives blocks.
type SinkSession[
	I BlockId,
	F Flags,
] struct {
	accumulator  StatusAccumulator[I]
	orchestrator Orchestrator[F]
	store        BlockStore[I]
	pending      *util.SynchronizedDeque[Block[I]]
	stats        stats.Stats
}

// Struct for returning summary information about the session
type SinkSessionInfo[F Flags] struct {
	PendingCount  uint
	Status        F
	HavesEstimate uint
	Wants         uint
}

func (inf *SinkSessionInfo[F]) String() string {
	return fmt.Sprintf("pnd:%6v have:%6v want:%6v %v", inf.PendingCount, inf.HavesEstimate, inf.Wants, inf.Status)
}

// NewSinkSession creates a new SinkSession.
func NewSinkSession[I BlockId, F Flags](
	store BlockStore[I],
	accumulator StatusAccumulator[I],
	orchestrator Orchestrator[F],
	stats stats.Stats,
) *SinkSession[I, F] {
	return &SinkSession[I, F]{
		accumulator,
		orchestrator,
		store,
		util.NewSynchronizedDeque[Block[I]](util.NewBlocksDeque[Block[I]](2048)),
		stats,
	}
}

// Get the current state from this session
func (ss *SinkSession[I, F]) State() F {
	return ss.orchestrator.State()
}

// Notify the session of some event which may change state
func (ss *SinkSession[I, F]) Notify(event SessionEvent) error {
	return ss.orchestrator.Notify(event)
}

// Get information about this session
func (ss *SinkSession[I, F]) Info() *SinkSessionInfo[F] {
	return &SinkSessionInfo[F]{
		PendingCount:  uint(ss.pending.Len()),
		Status:        ss.orchestrator.State(),
		HavesEstimate: ss.accumulator.HaveCount(),
		Wants:         ss.accumulator.WantCount(),
	}
}

// AccumulateStatus accumulates the status of the block with the given id and all of its children.
func (ss *SinkSession[I, F]) AccumulateStatus(id I) error {
	// Get block and handle errors
	block, err := ss.store.Get(context.Background(), id)

	if err == errors.ErrBlockNotFound {
		return ss.accumulator.Want(id)
	}

	if err != nil {
		return err
	}

	if err := ss.accumulator.Have(id); err != nil {
		return err
	}

	for _, child := range block.Children() {
		if err := ss.AccumulateStatus(child); err != nil {
			return err
		}
	}

	return nil
}

// Enqueue a block for transfer (will retrieve block and children from the related source session)
func (ss *SinkSession[I, F]) Enqueue(id I) error {
	// When is it safe to do this?
	// if we are currently checking (e.g. the polling loop is running)
	// if we are not currently checking *and* we are not currently sending?
	ss.orchestrator.Notify(BEGIN_ENQUEUE)     // This should block if status is RECEIVER_SENDING
	defer ss.orchestrator.Notify(END_ENQUEUE) // This should set RECEIVER_CHECKING
	return ss.AccumulateStatus(id)            // This is recursive so it may take some time
}

// HandleBlock handles a block that is being received.
func (ss *SinkSession[I, F]) HandleBlock(rawBlock RawBlock[I]) {
	ss.orchestrator.Notify(BEGIN_RECEIVE)
	defer ss.orchestrator.Notify(END_RECEIVE)

	block, err := ss.store.Add(context.TODO(), rawBlock)
	if err != nil {
		ss.stats.Logger().Errorf("Failed to add block to store. err = %v", err)
	}

	if err := ss.accumulator.Receive(block.Id()); err != nil {
		ss.stats.Logger().Errorf("Failed to receive block. err = %v", err)
	}

	ss.pending.PushBack(block)
}

// Run runs the receiver session.
// TODO: Is start a better name?  Starting a session?
// Or begin, to match the event names?
func (ss *SinkSession[I, F]) Run(sender StatusSender[I]) error {

	ss.orchestrator.Notify(BEGIN_SESSION)
	defer ss.orchestrator.Notify(END_SESSION)

	for !ss.orchestrator.IsClosed() {
		ss.orchestrator.Notify(BEGIN_CHECK)

		if ss.pending.Len() > 0 {
			block := ss.pending.PollFront()

			for _, child := range block.Children() {
				if err := ss.AccumulateStatus(child); err != nil {
					return err
				}
			}
		} else {
			// If we get here it means we have no more blocks to process. In a batch based process that's
			// the only time we'd want to send. But for streaming maybe this should be in a separate loop
			// so it can be triggered by the orchestrator - otherwise we wind up sending a status update every
			// time the pending list becomes empty.

			ss.orchestrator.Notify(BEGIN_SEND)
			ss.accumulator.Send(sender)
			ss.orchestrator.Notify(END_SEND)

		}
		ss.orchestrator.Notify(END_CHECK)
	}

	return nil
}

// Receive state from the remote session.
func (ss *SinkSession[I, F]) ReceiveState(state F) error {
	return ss.orchestrator.ReceiveState(state)
}

// Close closes the sender session.
func (ss *SinkSession[I, F]) Close() error {
	if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	defer ss.orchestrator.Notify(END_CLOSE)

	return nil
}

// Cancel cancels the session.
func (ss *SinkSession[I, F]) Cancel() error {
	return ss.orchestrator.Notify(CANCEL)
}

// IsClosed returns true if the session is closed.
func (ss *SinkSession[I, F]) IsClosed() bool {
	return ss.orchestrator.IsClosed()
}

// Struct for returting summary information about the session
type SourceSessionInfo[F Flags] struct {
	PendingCount  uint
	Status        F
	HavesEstimate uint
}

func (inf *SourceSessionInfo[F]) String() string {
	return fmt.Sprintf("pnd:%6v have:%6v %v", inf.PendingCount, inf.HavesEstimate, inf.Status)
}

// SourceSession is a session for sending blocks.
type SourceSession[
	I BlockId,
	F Flags,
] struct {
	store        BlockStore[I]
	orchestrator Orchestrator[F]
	filter       filter.Filter[I]
	sent         sync.Map
	pending      *util.SynchronizedDeque[I]
	stats        stats.Stats
}

// NewSourceSession creates a new SourceSession.
func NewSourceSession[I BlockId, F Flags](store BlockStore[I], filter filter.Filter[I], orchestrator Orchestrator[F], stats stats.Stats) *SourceSession[I, F] {
	return &SourceSession[I, F]{
		store,
		orchestrator,
		filter,
		sync.Map{},
		util.NewSynchronizedDeque[I](util.NewBlocksDeque[I](1024)),
		stats,
	}
}

// Retrieve the current session state
func (ss *SourceSession[I, F]) State() F {
	return ss.orchestrator.State()
}

// Notify the session of some event which may change state
func (ss *SourceSession[I, F]) Notify(event SessionEvent) error {
	return ss.orchestrator.Notify(event)
}

// Retrieve information about this session
func (ss *SourceSession[I, F]) Info() *SourceSessionInfo[F] {
	return &SourceSessionInfo[F]{
		PendingCount:  uint(ss.pending.Len()),
		Status:        ss.orchestrator.State(),
		HavesEstimate: uint(ss.filter.Count()),
	}
}

// Run runs the sender session.
// TODO: Consider renaming to Start or Begin or Open to match Close/IsClosed?
func (ss *SourceSession[I, F]) Run(sender BlockSender[I]) error {
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
	return ss.orchestrator.Notify(END_SESSION)
}

// HandleStatus handles incoming status, updating the filter and pending list.
// TODO: Is pending just want but from the sender side?  and filter is have from the sender side?  Could we name these similarly?
// receiverHave, receiverWant?
func (ss *SourceSession[I, F]) HandleStatus(have filter.Filter[I], want []I) {
	if err := ss.orchestrator.Notify(BEGIN_RECEIVE); err != nil {
		ss.stats.Logger().Errorw("error notifying BEGIN_RECEIVE", "object", "SourceSession", "method", "HandleStatus", "error", err)
	}
	defer ss.orchestrator.Notify(END_RECEIVE)
	ss.stats.Logger().Debugw("begin processing", "object", "SourceSession", "method", "HandleStatus", "pending", ss.pending.Len(), "filter", ss.filter.Count())
	ss.filter = ss.filter.AddAll(have)
	ss.stats.Logger().Debugw("incoming have filter merged", "object", "SourceSession", "method", "HandleStatus", "filter", ss.filter.Count())
	//ss.filter.Dump(ss.log, "SourceSession filter - ")
	for _, id := range want {
		ss.pending.PushFront(id) // send wants to the front of the queue
	}
	ss.stats.Logger().Debugw("incoming want list merged", "obect", "SourceSession", "method", "HandleStatus", "pending", ss.pending.Len())
}

// Close closes the sender session.
func (ss *SourceSession[I, F]) Close() error {
	if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	defer ss.orchestrator.Notify(END_CLOSE)

	// TODO: Clear the bloom filter

	return nil
}

// Cancel cancels the sender session.
func (ss *SourceSession[I, F]) Cancel() error {
	return ss.orchestrator.Notify(CANCEL)
}

// Enqueue enqueues a block id to be sent.
func (ss *SourceSession[I, F]) Enqueue(id I) error {
	ss.orchestrator.Notify(BEGIN_ENQUEUE)
	defer ss.orchestrator.Notify(END_ENQUEUE)
	return ss.pending.PushBack(id)
}

// HandleState handles incoming session state.
func (ss *SourceSession[I, F]) ReceiveState(state F) error {
	return ss.orchestrator.ReceiveState(state)
}

// IsClosed returns true if the session is closed.
func (ss *SourceSession[I, F]) IsClosed() bool {
	return ss.orchestrator.IsClosed()
}

// ByteAndBlockReader is an io.Reader that also implements io.ByteReader.
type ByteAndBlockReader interface {
	io.Reader
	io.ByteReader
}
