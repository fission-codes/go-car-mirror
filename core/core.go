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

// BlockSender is responsible for sending blocks from the Source, immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId] interface {
	// Send a block
	SendBlock(block RawBlock[I]) error
	// Ensure any blocks queued for sending are actually sent; should block until all blocks are sent.
	Flush() error
	// Close the sender gracefully, ensuring any pending blocks are flushed
	Close() error

	Len() int
}

// BlockReceiver is responsible for receiving blocks at the Sink.
type BlockReceiver[I BlockId] interface {
	// HandleBlock is called on receipt of a new block.
	HandleBlock(block RawBlock[I])
}

// StatusSender is responsible for sending status from the Sink.
// The key intuition of CAR Mirror is information about blocks already present on a Sink can be sent efficiently
// using a lossy filter. We also need to be able to request specific blocks from the source. 'Status' is therefore
// formally a Filter of blocks the source already have and a list of blocks the source definitely wants.
type StatusSender[I BlockId] interface {
	// SendStatus sends the status to the SourceSession.
	// The have filter is a lossy filter of the blocks that the SinkSession has.
	// The want list is a list of blocks that the SinkSession wants.
	SendStatus(have filter.Filter[I], want []I) error

	// Close the Status Sender
	Close() error
}

// StatusReceiver is responsible for receiving status on the Source.
type StatusReceiver[I BlockId] interface {
	// HandleStatus is called on receipt of a new status.
	// The have filter is a lossy filter of the blocks that the Sink has.
	// The want list is a list of blocks that the Sink wants.
	HandleStatus(have filter.Filter[I], want []I)
}

// StatusAccumulator is responsible for collecting status on the Sink so that it can be sent to the Source.
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
// When events occur, they trigger updates to session state. The Orchestrator, which is typically some object
// outside the session which is aware of implementation specific information about the channel over which communication
// is actually occuring, may also 'wait' on notification of an event.
type SessionEvent uint16

// Core session event constants.
const (
	BEGIN_SESSION    SessionEvent = iota // A session has begun
	END_SESSION                          // A session has ended
	BEGIN_DRAINING                       // No more data is currently available for processing
	END_DRAINING                         // Session has completed processing related to the draining event
	BEGIN_CLOSE                          // The session has been notified it should close when current processing is complete
	END_CLOSE                            // Immediate actions to start the session closing are complete
	BEGIN_FLUSH                          // Flush has been called on a SimpleBatchBlockSender to send a batch of blocks from Source to Sink
	END_FLUSH                            // Flush has completed
	BEGIN_SEND                           // On Source, an individual block has been sent to the BlockSender. On Sink, we are 'sending' status to the StatusAccumulator
	END_SEND                             // Send has completed
	BEGIN_RECEIVE                        // An individual block or status update is received
	END_RECEIVE                          // Receive operation is completed
	BEGIN_PROCESSING                     // Begin main processing loop
	END_PROCESSING                       // End main procesing loop
	// TODO: Change comments to allow BATCH to be about blocks and status too.
	BEGIN_BATCH   // SimpleBatchBlockReceiver has started processing a batch of blocks on the Sink
	END_BATCH     // Finished processing a batch of blocks
	BEGIN_ENQUEUE // Request made to start transfer of a specific block and its children
	END_ENQUEUE   // Enqueue is complete
	CANCEL        // Request made to immediately end session and abandon any current transfer
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
	case BEGIN_PROCESSING:
		return "BEGIN_PROCESSING"
	case END_PROCESSING:
		return "END_PROCESSING"
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
// Typically the Orchestrator is some external object such as a connection which is aware of details which are specific
// to the implementation (such as whether the communication is synchronous or batch oriented).
type Orchestrator[F Flags] interface {
	// Notify is used to notify the orchestrator of an event.
	Notify(SessionEvent) error
	// Get the current state of the Orchestrator.
	State() F
	// IsClosed returns true if the orchestrator is closed.
	IsClosed() bool

	// IsSafeStateToClose returns true if the state of the Orchestrator indicates that the session should close.
	// Other factors may need to be taken into account as well to determine if close should happen, but this
	// tells us if the states indicate a close is safe.
	// TODO: Maybe rename to IsSafeToClose or IsSafeCloseState?
	IsSafeStateToClose() bool
}

// SinkSession is a session that receives blocks and sends out status updates. Two type parameters
// must be supplied, which are the concrete type of the BlockId and the session State
type SinkSession[
	I BlockId, // Concrete type for the Block Id used by this session
	F Flags, // Concrete type for the state information managed by the Orchestrator
] struct {
	statusAccumulator StatusAccumulator[I]
	orchestrator      Orchestrator[F]
	store             BlockStore[I]
	pendingBlocks     *util.SynchronizedDeque[Block[I]]
	stats             stats.Stats
	startedCh         chan bool
	doneCh            chan error
	requester         bool // requester or responder
}

// Struct for returning summary information about the session. This is intended to allow implementers to
// provide users with information concerning transfers in progress.
type SinkSessionInfo[F Flags] struct {
	PendingBlocksCount uint // Number of received blocks currently pending processing
	State              F    // State from Orchestrator
	HavesEstimate      uint // Estimate of the number of 'haves' accumulated since status last sent
	Wants              uint // Number of 'wants' accumulated since status last sent
}

// Default string representation of SinkSessionInfo
func (inf *SinkSessionInfo[F]) String() string {
	return fmt.Sprintf("pnd:%6v have:%6v want:%6v %v", inf.PendingBlocksCount, inf.HavesEstimate, inf.Wants, inf.State)
}

// NewSinkSession creates a new SinkSession.
func NewSinkSession[I BlockId, F Flags](
	store BlockStore[I], // Block store to which received blocks will be written
	statusAccumulator StatusAccumulator[I], // Accumulator for status information
	orchestrator Orchestrator[F], // Orchestrator used to synchronize this session with its communication channel
	stats stats.Stats, // Collector for session-related statistics
	requester bool,
) *SinkSession[I, F] {
	return &SinkSession[I, F]{
		statusAccumulator,
		orchestrator,
		store,
		util.NewSynchronizedDeque[Block[I]](util.NewBlocksDeque[Block[I]](2048)),
		stats,
		make(chan bool, 1),
		make(chan error, 1),
		requester,
	}
}

// Done returns an error channel which will be closed when the session is complete.
func (ss *SinkSession[I, F]) Done() <-chan error {
	return ss.doneCh
}

// Started returns a bool channel which will receive a value when the session has started.
func (ss *SinkSession[I, F]) Started() <-chan bool {
	return ss.startedCh
}

// Get the orchestrator for this session
func (ss *SinkSession[I, F]) Orchestrator() Orchestrator[F] {
	return ss.orchestrator
}

// Get information about this session
func (ss *SinkSession[I, F]) Info() *SinkSessionInfo[F] {
	return &SinkSessionInfo[F]{
		PendingBlocksCount: uint(ss.pendingBlocks.Len()),
		State:              ss.orchestrator.State(),
		HavesEstimate:      ss.statusAccumulator.HaveCount(),
		Wants:              ss.statusAccumulator.WantCount(),
	}
}

// Accumulates the status of the block with the given id and all of its children.
func (ss *SinkSession[I, F]) AccumulateStatus(id I) error {
	// Get block and handle errors
	block, err := ss.store.Get(context.Background(), id)

	if err == errors.ErrBlockNotFound {
		return ss.statusAccumulator.Want(id)
	}

	if err != nil {
		return err
	}

	if err := ss.statusAccumulator.Have(id); err != nil {
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
	if err := ss.orchestrator.Notify(BEGIN_ENQUEUE); err != nil { // This should block if state is RECEIVER_SENDING
		return err
	}

	defer ss.orchestrator.Notify(END_ENQUEUE) // This should set RECEIVER_CHECKING

	return ss.AccumulateStatus(id) // This is recursive so it may take some time
}

// HandleBlock handles a block that is being received. Adds the block to the session's store, and
// then queues the block for further processing.
func (ss *SinkSession[I, F]) HandleBlock(rawBlock RawBlock[I]) {
	ss.orchestrator.Notify(BEGIN_RECEIVE)
	defer ss.orchestrator.Notify(END_RECEIVE)

	block, err := ss.store.Add(context.TODO(), rawBlock)
	if err != nil {
		ss.stats.Logger().Errorf("Failed to add block to store. err = %v", err)
	}

	if err := ss.statusAccumulator.Receive(block.Id()); err != nil {
		ss.stats.Logger().Errorf("Failed to receive block. err = %v", err)
	}

	ss.pendingBlocks.PushBack(block)
}

// Runs the receiver session. Pulls blocks from the queue of received blocks, then checks the block descendents
// to see if any are already present in the store, accumulating status accordingly. Terminates when the orchestrator's
// IsClosed method returns true.
func (ss *SinkSession[I, F]) Run(
	statusSender StatusSender[I], // Sender used to transmit status to source
) {
	defer func() {
		log.Debugw("SinkSession.Run() exiting")
		close(ss.doneCh)
		log.Debugw("SinkSession.Run() exited")

		// log.Debugw("SinkSession.Run() closing statusSender")
		// statusSender.Close()
		// log.Debugw("SinkSession.Run() closed statusSender")
	}()
	ss.startedCh <- true
	close(ss.startedCh)

	if err := ss.orchestrator.Notify(BEGIN_SESSION); err != nil {
		ss.doneCh <- err
		return
	}
	defer ss.orchestrator.Notify(END_SESSION)

	for !ss.orchestrator.IsClosed() {
		if ss.pendingBlocks.Len() == 0 && ss.statusAccumulator.WantCount() == 0 && ss.orchestrator.IsSafeStateToClose() {
			if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
				ss.doneCh <- err
				return
			}
			if err := ss.orchestrator.Notify(END_CLOSE); err != nil {
				ss.doneCh <- err
				return
			}
		}

		if err := ss.orchestrator.Notify(BEGIN_PROCESSING); err != nil {
			ss.doneCh <- err
			return
		}

		// Pending blocks are blocks that have been recieved and added to our block store,
		// but have not had status accumulated on their children yet.
		if ss.pendingBlocks.Len() > 0 {
			// TODO: Why is this sending?  We're only accumulating status here.
			// Per docs, SINK_SENDING means we might have status pending flush.
			// That could be true regardless of if there are pending blocks though, right?
			// Feels like confusing naming.
			if err := ss.orchestrator.Notify(BEGIN_SEND); err != nil {
				ss.doneCh <- err
				return
			}

			block := ss.pendingBlocks.PollFront()

			for _, child := range block.Children() {
				if err := ss.AccumulateStatus(child); err != nil {
					ss.doneCh <- err
					return
				}
			}
			if err := ss.orchestrator.Notify(END_SEND); err != nil {
				ss.doneCh <- err
				return
			}
		} else {
			// If we get here it means we have no more blocks to process. In a batch based process that's
			// the only time we'd want to send. But for streaming maybe this should be in a separate loop
			// so it can be triggered by the orchestrator - otherwise we wind up sending a status update every
			// time the pending blocks list becomes empty.

			if err := ss.orchestrator.Notify(BEGIN_DRAINING); err != nil {
				ss.doneCh <- err
				return
			}
			if err := ss.statusAccumulator.Send(statusSender); err != nil {
				ss.doneCh <- err
				return
			}
			if err := ss.orchestrator.Notify(END_DRAINING); err != nil {
				ss.doneCh <- err
				return
			}
		}
		if err := ss.orchestrator.Notify(END_PROCESSING); err != nil {
			ss.doneCh <- err
			return
		}
	}
}

// Closes the sink session. Note that the session does not close immediately; this method will return before
// the session is closed. Exactly *when* the session closes is determined by the orchestrator, but in general
// this should be only after the session has completed all transfers which are currently in progresss.
func (ss *SinkSession[I, F]) Close() error {
	if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	return ss.orchestrator.Notify(END_CLOSE)
}

// Cancel cancels the session. The session does not *immediately* terminate; the orchestrator plays a role
// in deciding this. However, transfers in progress will not usually complete.
func (ss *SinkSession[I, F]) Cancel() error {
	return ss.orchestrator.Notify(CANCEL)
}

// IsClosed returns true if the session is closed.
func (ss *SinkSession[I, F]) IsClosed() bool {
	return ss.orchestrator.IsClosed()
}

// Struct for returting summary information about a Source Session. This is allows implementers to provide
// user feedback on the state of in-progress transfers.
type SourceSessionInfo[F Flags] struct {
	PendingBlocksCount uint // Number of blocks awaiting transmission
	State              F    // Internal sessions state provided by the Orchestraotr
	HavesEstimate      uint // Estimate of the number of specific block Ids we know are present on the Sink
}

// Default user-friendly representation of SourceSessionInfo
func (inf *SourceSessionInfo[F]) String() string {
	return fmt.Sprintf("pnd:%6v have:%6v %v", inf.PendingBlocksCount, inf.HavesEstimate, inf.State)
}

// SinkSession is a session that sends blocks and receives status updates. Two type parameters
// must be supplied, which are the concrete type of the BlockId and the session State
type SourceSession[
	I BlockId,
	F Flags,
] struct {
	store         BlockStore[I]
	orchestrator  Orchestrator[F]
	filter        filter.Filter[I]
	sent          sync.Map
	pendingBlocks *util.SynchronizedDeque[I]
	stats         stats.Stats
	startedCh     chan bool
	doneCh        chan error
	requester     bool
}

// NewSourceSession creates a new SourceSession.
func NewSourceSession[I BlockId, F Flags](
	store BlockStore[I], // Block store from which blocks are read
	filter filter.Filter[I], // Filter to which 'Haves' from the sink will be added
	orchestrator Orchestrator[F], // Orchestrator used to synchronize this session with its communication channel
	stats stats.Stats, // Collector for session-related statistics
	requester bool, // Requester or responder
) *SourceSession[I, F] {
	return &SourceSession[I, F]{
		store,
		orchestrator,
		filter,
		sync.Map{},
		util.NewSynchronizedDeque[I](util.NewBlocksDeque[I](1024)),
		stats,
		make(chan bool, 1),
		make(chan error, 1),
		requester,
	}
}

// Done returns an error channel that will be closed when the session is closed.
// If the session is closed due to an error, the error will be sent on the channel.
func (ss *SourceSession[I, F]) Done() <-chan error {
	return ss.doneCh
}

// Started returns a channel that will be closed when the session is started.
func (ss *SourceSession[I, F]) Started() <-chan bool {
	return ss.startedCh
}

// Retrieve the current session state
func (ss *SourceSession[I, F]) Orchestrator() Orchestrator[F] {
	return ss.orchestrator
}

// Retrieve information about this session
func (ss *SourceSession[I, F]) Info() *SourceSessionInfo[F] {
	return &SourceSessionInfo[F]{
		PendingBlocksCount: uint(ss.pendingBlocks.Len()),
		State:              ss.orchestrator.State(),
		HavesEstimate:      uint(ss.filter.Count()),
	}
}

// Runs the sender session. Polls the queue of pending blocks, and sends the next block and then queues
// the children of that block to be sent. Only sends a block if it does not match the Filter, or if it
// is specifically requested via Enqueue or included as a 'want' in received status.
func (ss *SourceSession[I, F]) Run(
	blockSender BlockSender[I], // Sender used to sent blocks to sink
) {
	defer func() {
		log.Debugw("SourceSession.Run() exiting")
		close(ss.doneCh)
		log.Debugw("SourceSession.Run() exited")

		// log.Debugw("SourceSession.Run() closing blockSender")
		// blockSender.Close()
		// log.Debugw("SourceSession.Run() closed blockSender")
	}()
	ss.startedCh <- true
	close(ss.startedCh)

	if err := ss.orchestrator.Notify(BEGIN_SESSION); err != nil {
		ss.doneCh <- err
		return
	}

	// TODO: Change the criteria for exiting the loop
	// If we have no pending blocks, and our blockSender has no blocks, and we aren't flushing, and we aren't receiving, we're done.
	// If we're done flushing, that means we aren't receiving.
	// Flushing will happen if this test is false, so we only have to account for length of blocksender blocks.
	for !ss.orchestrator.IsClosed() {

		// Note that this check means you must enqueue before running a session.
		// TODO: Is there any need to mutex this so we get it for both variables simultaneously?
		log.Debugw("SourceSession.Run() checking for pending blocks", "pendingBlocks", ss.pendingBlocks.Len(), "blockSender", blockSender.Len())
		if ss.pendingBlocks.Len() == 0 && blockSender.Len() == 0 && ss.orchestrator.IsSafeStateToClose() {
			if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
				ss.doneCh <- err
				return
			}
			if err := ss.orchestrator.Notify(END_CLOSE); err != nil {
				ss.doneCh <- err
				return
			}
		}

		if err := ss.orchestrator.Notify(BEGIN_PROCESSING); err != nil {
			ss.doneCh <- err
			return
		}

		if ss.pendingBlocks.Len() > 0 {
			id := ss.pendingBlocks.PollFront()
			if _, ok := ss.sent.Load(id); !ok {
				// TODO: Is this safe?  If we don't find it in the store, we already marked it as sent.
				ss.sent.Store(id, true)

				block, err := ss.store.Get(context.Background(), id)
				if err != nil {
					if err != errors.ErrBlockNotFound {
						ss.doneCh <- err
						return
					}
					// TODO: How do we notify that the request block was not found?
				} else {
					if err := ss.orchestrator.Notify(BEGIN_SEND); err != nil {
						ss.doneCh <- err
						return
					}
					if err := blockSender.SendBlock(block); err != nil {
						// TODO: If error here, maybe we should remove it from ss.sent.
						ss.doneCh <- err
						return
					}
					for _, child := range block.Children() {
						if ss.filter.DoesNotContain(child) {
							if err := ss.pendingBlocks.PushBack(child); err != nil {
								ss.doneCh <- err
								return
							}
						}
					}
					if err := ss.orchestrator.Notify(END_SEND); err != nil {
						ss.doneCh <- err
						return
					}
				}
			}
		} else {
			if err := ss.orchestrator.Notify(BEGIN_DRAINING); err != nil {
				ss.doneCh <- err
				return
			}
			if err := blockSender.Flush(); err != nil {
				ss.doneCh <- err
				return
			}
			if err := ss.orchestrator.Notify(END_DRAINING); err != nil {
				ss.doneCh <- err
				return
			}
		}
		if err := ss.orchestrator.Notify(END_PROCESSING); err != nil {
			ss.doneCh <- err
			return
		}
	}
	if err := ss.orchestrator.Notify(END_SESSION); err != nil {
		ss.doneCh <- err
		return
	}
}

// HandleStatus handles incoming status, updating the filter and pending blocks list.
func (ss *SourceSession[I, F]) HandleStatus(
	have filter.Filter[I], // A collection of blocks known to already exist on the sink
	want []I, // A collection of blocks known not to exist on the sink
) {
	if err := ss.orchestrator.Notify(BEGIN_RECEIVE); err != nil {
		ss.stats.Logger().Errorw("error notifying BEGIN_RECEIVE", "object", "SourceSession", "method", "HandleStatus", "error", err)
	}
	defer ss.orchestrator.Notify(END_RECEIVE)
	ss.stats.Logger().Debugw("begin processing", "object", "SourceSession", "method", "HandleStatus", "pendingBlocks", ss.pendingBlocks.Len(), "filter", ss.filter.Count())
	// TODO: Had a race condition.  Write on line below, but read on 704.
	// ss.filter = ss.filter.AddAll(have)
	// This looks like what we want though.
	ss.filter.AddAll(have)
	ss.stats.Logger().Debugw("incoming have filter merged", "object", "SourceSession", "method", "HandleStatus", "filter", ss.filter.Count())
	//ss.filter.Dump(ss.log, "SourceSession filter - ")
	for _, id := range want {
		ss.pendingBlocks.PushFront(id) // send wants to the front of the queue
	}
	ss.stats.Logger().Debugw("incoming want list merged", "obect", "SourceSession", "method", "HandleStatus", "pendingBlocks", ss.pendingBlocks.Len())
}

// Closes the source session. Note that the session does not close immediately; this method will return before
// the session is closed. Exactly *when* the session closes is determined by the orchestrator, but in general
// this should be only after the session has completed all transfers which are currently in progresss.
func (ss *SourceSession[I, F]) Close() error {
	if err := ss.orchestrator.Notify(BEGIN_CLOSE); err != nil {
		return err
	}
	defer ss.orchestrator.Notify(END_CLOSE)

	// TODO: Clear the bloom filter

	return nil
}

// Cancel cancels the session. The session does not *immediately* terminate; the orchestrator plays a role
// in deciding this. However, transfers in progress will not usually complete.
func (ss *SourceSession[I, F]) Cancel() error {
	return ss.orchestrator.Notify(CANCEL)
}

// Enqueue enqueues a block id to be sent.
func (ss *SourceSession[I, F]) Enqueue(id I) error {
	ss.orchestrator.Notify(BEGIN_ENQUEUE)
	defer ss.orchestrator.Notify(END_ENQUEUE)
	return ss.pendingBlocks.PushBack(id)
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
