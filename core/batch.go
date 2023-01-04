package core

import (
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/util"
	"go.uber.org/zap"
)

// BatchState is a bitfield that describes the state of a batch session.
type BatchState uint32

// Named constants for BatchState flags.
const (
	SINK_READY BatchState = 1 << iota
	SINK_CLOSING
	SINK_CLOSED
	SINK_CHECKING   // Sink is processing a batch of blocks
	SINK_SENDING    // Sink is in the process of sending a status message
	SINK_WAITING    // Sink is waiting for a batch of blocks
	SINK_ENQUEUING  // Sink is processing a block id that has been explicitly requested
	SOURCE_READY    // Source is processing a batch of blocks
	SOURCE_FLUSHING // Source is flushing blocks
	SOURCE_WAITING  // Source is waiting for a status message
	SOURCE_CLOSING
	SOURCE_CLOSED
	CANCELLED
	SINK   = CANCELLED | SINK_READY | SINK_CLOSING | SINK_CHECKING | SINK_CLOSED | SINK_SENDING | SINK_ENQUEUING | SINK_WAITING
	SOURCE = CANCELLED | SOURCE_READY | SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_CLOSING | SOURCE_CLOSED
)

// Strings returns a slice of strings describing the given BatchState.
func (bs BatchState) Strings() []string {
	var strings []string
	if bs&SINK_READY != 0 {
		strings = append(strings, "SINK_READY")
	}
	if bs&SINK_CLOSING != 0 {
		strings = append(strings, "SINK_CLOSING")
	}
	if bs&SINK_CLOSED != 0 {
		strings = append(strings, "SINK_CLOSED")
	}
	if bs&SINK_CHECKING != 0 {
		strings = append(strings, "SINK_CHECKING")
	}
	if bs&SINK_SENDING != 0 {
		strings = append(strings, "SINK_SENDING")
	}
	if bs&SINK_WAITING != 0 {
		strings = append(strings, "SINK_WAITING")
	}
	if bs&SINK_ENQUEUING != 0 {
		strings = append(strings, "SINK_ENQUEUING")
	}
	if bs&SOURCE_READY != 0 {
		strings = append(strings, "SOURCE_READY")
	}
	if bs&SOURCE_CLOSING != 0 {
		strings = append(strings, "SOURCE_CLOSING")
	}
	if bs&SOURCE_CLOSED != 0 {
		strings = append(strings, "SOURCE_CLOSED")
	}
	if bs&CANCELLED != 0 {
		strings = append(strings, "CANCELLED")
	}
	return strings
}

// String returns a string describing the given BatchState.
// Each state flag is separated by a pipe character.
func (bs BatchState) String() string {
	return strings.Join(bs.Strings(), "|")
}

// BatchBlockReceiver is an interface for receiving batches of blocks.
type BatchBlockReceiver[I BlockId] interface {
	// HandleList handles a list of blocks.
	HandleList(BatchState, []RawBlock[I]) error
}

// BatchBlockSender is an interface for sending batches of blocks.
type BatchBlockSender[I BlockId] interface {
	// SendList sends a list of blocks.
	SendList(BatchState, []RawBlock[I]) error
	// Close closes the sender.
	Close() error
}

// SimpleBatchBlockReceiver is a simple implementation of BatchBlockReceiver.
type SimpleBatchBlockReceiver[I BlockId] struct {
	session      BlockReceiver[I, BatchState]
	orchestrator Orchestrator[BatchState]
}

// NewSimpleBatchBlockReceiver creates a new SimpleBatchBlockReceiver.
func NewSimpleBatchBlockReceiver[I BlockId](rs BlockReceiver[I, BatchState], orchestrator Orchestrator[BatchState]) *SimpleBatchBlockReceiver[I] {
	return &SimpleBatchBlockReceiver[I]{
		session:      rs,
		orchestrator: orchestrator,
	}
}

// HandleList handles a list of raw blocks.
func (sbbr *SimpleBatchBlockReceiver[I]) HandleList(flags BatchState, list []RawBlock[I]) error {
	sbbr.orchestrator.Notify(BEGIN_BATCH)
	defer sbbr.orchestrator.Notify(END_BATCH)
	for _, block := range list {
		sbbr.session.HandleBlock(block)
	}
	sbbr.session.HandleState(flags)
	return nil
}

// SimpleBatchBlockSender is a simple implementation of BlockSender which wraps a BatchBlockSender
type SimpleBatchBlockSender[I BlockId] struct {
	orchestrator Orchestrator[BatchState]
	list         []RawBlock[I]
	listMutex    sync.Mutex
	sender       BatchBlockSender[I]
	maxBatchSize uint32
}

// NewSimpleBatchBlockSender creates a new SimpleBatchBlockSender.
func NewSimpleBatchBlockSender[I BlockId](sender BatchBlockSender[I], orchestrator Orchestrator[BatchState], maxBatchSize uint32) *SimpleBatchBlockSender[I] {
	return &SimpleBatchBlockSender[I]{
		orchestrator: orchestrator,
		list:         make([]RawBlock[I], 0, maxBatchSize),
		sender:       sender,
		maxBatchSize: maxBatchSize,
	}
}

// SendBlock adds a block to the batch and sends the batch if it is full.
func (sbbs *SimpleBatchBlockSender[I]) SendBlock(block RawBlock[I]) error {
	sbbs.listMutex.Lock()
	sbbs.list = append(sbbs.list, block)
	sbbs.listMutex.Unlock()

	if len(sbbs.list) >= int(sbbs.maxBatchSize) {
		return sbbs.Flush()
	}

	return nil
}

// Close closes the sender.
func (sbbs *SimpleBatchBlockSender[I]) Close() error {
	return sbbs.sender.Close()
}

// Flush sends the current batch of blocks.
func (sbbs *SimpleBatchBlockSender[I]) Flush() error {
	sbbs.orchestrator.Notify(BEGIN_FLUSH)
	defer sbbs.orchestrator.Notify(END_FLUSH)

	batchState := sbbs.orchestrator.State()
	sbbs.listMutex.Lock()
	defer sbbs.listMutex.Unlock()
	if err := sbbs.sender.SendList(batchState&SOURCE, sbbs.list); err != nil {
		return err
	}
	sbbs.list = sbbs.list[:0]

	return nil
}

// BatchSourceOrchestrator is an orchestrator for sending batches of blocks.
type BatchSourceOrchestrator struct {
	flags util.SharedFlagSet[BatchState]
	log   *zap.SugaredLogger
}

// NewBatchSourceOrchestrator creates a new BatchSendOrchestrator.
func NewBatchSourceOrchestrator() *BatchSourceOrchestrator {
	return &BatchSourceOrchestrator{
		flags: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchSendOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event.
// Events lead to state transitions in the orchestrator.
//
func (bso *BatchSourceOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		// bso.flags.Set(SOURCE_READY) - now set by END_ENQUEUE
	case BEGIN_SEND:
		state := bso.flags.WaitAny(SOURCE_READY|CANCELLED, 0)
		if state&CANCELLED != 0 {
			bso.log.Errorf("Orchestrator waiting for SOURCE_READY when CANCELLED seen")
			return errors.ErrStateError
		}
	case END_RECEIVE:
		bso.flags.Update(SOURCE_WAITING, SOURCE_READY)
	case BEGIN_CLOSE:
		bso.flags.Set(SOURCE_CLOSING)
	case BEGIN_FLUSH:
		bso.flags.Update(SOURCE_READY, SOURCE_FLUSHING)
	case END_FLUSH:
		bso.flags.Update(SOURCE_FLUSHING, SOURCE_WAITING)
	case BEGIN_DRAINING:
		if bso.flags.ContainsAny(SINK_CLOSING | SOURCE_CLOSING) {
			bso.flags.Set(SOURCE_CLOSED)
		}
	case END_ENQUEUE:
		// This is necessary because if the session is quiescent (e.g. has no in-flight exchange)
		// the session Run loop will be waiting on SOURCE_READY, and we need to set it to wake
		// up the session.  If the receiver is not flushing, waiting, or ready, it is quiescent.
		if !bso.flags.ContainsAny(SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_READY) {
			bso.flags.Set(SOURCE_READY)
		}
	case END_SESSION:
		bso.flags.Update(SOURCE, SOURCE_CLOSED)
	case CANCEL:
		bso.flags.Update(SOURCE, CANCELLED)
	}

	return nil
}

// State returns the current state of the orchestrator.
func (bso *BatchSourceOrchestrator) State() BatchState {
	return bso.flags.All()
}

// ReceiveState unsets any current receiver state flags, and sets the specified state flags.
func (bso *BatchSourceOrchestrator) ReceiveState(batchState BatchState) error {
	bso.flags.Update(SINK, batchState)
	return nil
}

// IsClosed returns true if the sender is closed.
func (bso *BatchSourceOrchestrator) IsClosed() bool {
	return bso.flags.Contains(SOURCE_CLOSED|SINK_CLOSED) || bso.flags.Contains(CANCELLED)
}

// BatchSinkOrchestrator is an orchestrator for receiving batches of blocks.
type BatchSinkOrchestrator struct {
	flags util.SharedFlagSet[BatchState]
	log   *zap.SugaredLogger
}

// NewBatchSinkOrchestrator creates a new BatchReceiveOrchestrator.
func NewBatchSinkOrchestrator() *BatchSinkOrchestrator {
	return &BatchSinkOrchestrator{
		flags: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchReceiveOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event, updating the state as appropriate.
func (bro *BatchSinkOrchestrator) Notify(event SessionEvent) error {
	// TODO: at this point we probably need to enclose all this in a mutex.
	switch event {
	case BEGIN_SESSION:
		bro.flags.Set(SINK_READY)
	case END_SESSION:
		bro.flags.Update(SINK_READY, SINK_CLOSED)
	case BEGIN_CLOSE:
		bro.flags.Set(SINK_CLOSING)
	case BEGIN_CHECK:
		state := bro.flags.WaitAny(SINK_CHECKING|CANCELLED, 0) // waits for either flag to be set
		if state&CANCELLED != 0 {
			bro.log.Errorf("Orchestrator waiting for SINK_CHECKING when CANCELLED seen")
			return errors.ErrStateError
		}
	case BEGIN_SEND:
		bro.flags.WaitExact(SINK_ENQUEUING, 0) // Can't send while enqueuing
		bro.flags.Update(SINK_CHECKING, SINK_SENDING)
		if bro.flags.Contains(SOURCE_CLOSED) {
			bro.flags.Set(SINK_CLOSED)
		}
	case END_BATCH:
		bro.flags.Update(SINK_WAITING, SINK_CHECKING)
	case END_SEND:
		bro.flags.Update(SINK_SENDING, SINK_WAITING)
	case CANCEL:
		bro.flags.Update(SINK, CANCELLED)
	case BEGIN_ENQUEUE:
		bro.flags.WaitExact(SINK_SENDING, 0) // Can't enqueue while sending
		bro.flags.Set(SINK_ENQUEUING)
	case END_ENQUEUE:
		bro.flags.Unset(SINK_ENQUEUING)
		// This is necessary because if the session is quiescent (e.g. has no in-flight exchange)
		// the session Run loop will be waiting on SINK_CHECKING, and we need to set it to send
		// an initial 'want' message to the block source. If the receiver is not sending, waiting,
		// or checking, it is quiescent.
		if !bro.flags.ContainsAny(SINK_SENDING | SINK_WAITING | SINK_CHECKING) {
			bro.flags.Set(SINK_CHECKING)
		}
	}

	return nil
}

// State returns the current state of the orchestrator.
func (bro *BatchSinkOrchestrator) State() BatchState {
	return bro.flags.All()
}

// ReceiveState unsets any current sender state flags, and sets the specified state flags.
func (bro *BatchSinkOrchestrator) ReceiveState(batchState BatchState) error {
	bro.flags.Update(SOURCE, batchState)
	return nil
}

// IsClosed returns true if the receiver is closed.
func (bro *BatchSinkOrchestrator) IsClosed() bool {
	return bro.flags.Contains(SOURCE_CLOSED|SINK_CLOSED) || bro.flags.Contains(CANCELLED)
}
