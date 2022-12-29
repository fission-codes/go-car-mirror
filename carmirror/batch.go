package carmirror

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
	RECEIVER_READY BatchState = 1 << iota
	RECEIVER_CLOSING
	RECEIVER_CLOSED
	RECEIVER_CHECKING  // Receiver is processing a batch of blocks
	RECEIVER_SENDING   // Receiver is in the process of sending a status message
	RECEIVER_WAITING   // Receiver is waiting for a batch of blocks
	RECEIVER_ENQUEUING // Receiver is processing a block id that has been explicitly requested
	SENDER_READY
	SENDER_CLOSING
	SENDER_CLOSED
	CANCELLED
	RECEIVER = CANCELLED | RECEIVER_READY | RECEIVER_CLOSING | RECEIVER_CHECKING | RECEIVER_CLOSED | RECEIVER_SENDING | RECEIVER_ENQUEUING | RECEIVER_WAITING
	SENDER   = CANCELLED | SENDER_READY | SENDER_CLOSING | SENDER_CLOSED
)

// Strings returns a slice of strings describing the given BatchState.
func (bs BatchState) Strings() []string {
	var strings []string
	if bs&RECEIVER_READY != 0 {
		strings = append(strings, "RECEIVER_READY")
	}
	if bs&RECEIVER_CLOSING != 0 {
		strings = append(strings, "RECEIVER_CLOSING")
	}
	if bs&RECEIVER_CLOSED != 0 {
		strings = append(strings, "RECEIVER_CLOSED")
	}
	if bs&RECEIVER_CHECKING != 0 {
		strings = append(strings, "RECEIVER_CHECKING")
	}
	if bs&RECEIVER_SENDING != 0 {
		strings = append(strings, "RECEIVER_SENDING")
	}
	if bs&RECEIVER_WAITING != 0 {
		strings = append(strings, "RECEIVER_WAITING")
	}
	if bs&RECEIVER_ENQUEUING != 0 {
		strings = append(strings, "RECEIVER_ENQUEUING")
	}
	if bs&SENDER_READY != 0 {
		strings = append(strings, "SENDER_READY")
	}
	if bs&SENDER_CLOSING != 0 {
		strings = append(strings, "SENDER_CLOSING")
	}
	if bs&SENDER_CLOSED != 0 {
		strings = append(strings, "SENDER_CLOSED")
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
	if err := sbbs.sender.SendList(batchState&SENDER, sbbs.list); err != nil {
		return err
	}
	sbbs.list = sbbs.list[:0]

	return nil
}

// BatchSendOrchestrator is an orchestrator for sending batches of blocks.
type BatchSendOrchestrator struct {
	flags util.SharedFlagSet[BatchState]
	log   *zap.SugaredLogger
}

// NewBatchSendOrchestrator creates a new BatchSendOrchestrator.
func NewBatchSendOrchestrator() *BatchSendOrchestrator {
	return &BatchSendOrchestrator{
		flags: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchSendOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event.
// Events lead to state transitions in the orchestrator.
func (bso *BatchSendOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		bso.flags.Set(SENDER_READY)
	case BEGIN_SEND:
		state := bso.flags.WaitAny(SENDER_READY|CANCELLED, 0)
		if state&CANCELLED != 0 {
			bso.log.Errorf("Orchestrator waiting for SENDER_READY when CANCELLED seen")
			return errors.ErrStateError
		}
	case END_RECEIVE:
		bso.flags.Set(SENDER_READY)
	case BEGIN_CLOSE:
		bso.flags.Set(SENDER_CLOSING)
	case BEGIN_FLUSH:
		bso.flags.Unset(SENDER_READY)
	case BEGIN_DRAINING:
		if bso.flags.ContainsAny(RECEIVER_CLOSED | SENDER_CLOSING) {
			bso.flags.Set(SENDER_CLOSED)
		}
	case END_SESSION:
		bso.flags.Update(SENDER, SENDER_CLOSED)
	case CANCEL:
		bso.flags.Update(SENDER, CANCELLED)
	}

	return nil
}

// State returns the current state of the orchestrator.
func (bso *BatchSendOrchestrator) State() BatchState {
	return bso.flags.All()
}

// ReceiveState unsets any current receiver state flags, and sets the specified state flags.
func (bso *BatchSendOrchestrator) ReceiveState(batchState BatchState) error {
	bso.flags.Update(RECEIVER, batchState)
	return nil
}

// IsClosed returns true if the sender is closed.
func (bso *BatchSendOrchestrator) IsClosed() bool {
	return bso.flags.Contains(SENDER_CLOSED|RECEIVER_CLOSED) || bso.flags.Contains(CANCELLED)
}

// BatchReceiveOrchestrator is an orchestrator for receiving batches of blocks.
type BatchReceiveOrchestrator struct {
	flags util.SharedFlagSet[BatchState]
	log   *zap.SugaredLogger
}

// NewBatchReceiveOrchestrator creates a new BatchReceiveOrchestrator.
func NewBatchReceiveOrchestrator() *BatchReceiveOrchestrator {
	return &BatchReceiveOrchestrator{
		flags: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchReceiveOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event, updating the state as appropriate.
func (bro *BatchReceiveOrchestrator) Notify(event SessionEvent) error {
	// TODO: at this point we probably need to enclose all this in a mutex.
	switch event {
	case BEGIN_SESSION:
		bro.flags.Set(RECEIVER_READY)
	case END_SESSION:
		bro.flags.Update(RECEIVER_READY, RECEIVER_CLOSED)
	case BEGIN_CLOSE:
		bro.flags.Set(RECEIVER_CLOSING)
	case BEGIN_CHECK:
		state := bro.flags.WaitAny(RECEIVER_CHECKING|CANCELLED, 0) // waits for either flag to be set
		if state&CANCELLED != 0 {
			bro.log.Errorf("Orchestrator waiting for RECEIVER_CHECKING when CANCELLED seen")
			return errors.ErrStateError
		}
	case BEGIN_SEND:
		bro.flags.WaitExact(RECEIVER_ENQUEUING, 0) // Can't send while enqueuing
		bro.flags.Update(RECEIVER_CHECKING, RECEIVER_SENDING)
		if bro.flags.ContainsAny(SENDER_CLOSED | RECEIVER_CLOSING) {
			bro.flags.Set(RECEIVER_CLOSED)
		}
	case END_BATCH:
		bro.flags.Update(RECEIVER_WAITING, RECEIVER_CHECKING)
	case END_SEND:
		bro.flags.Update(RECEIVER_SENDING, RECEIVER_WAITING)
	case CANCEL:
		bro.flags.Update(RECEIVER, CANCELLED)
	case BEGIN_ENQUEUE:
		bro.flags.WaitExact(RECEIVER_SENDING, 0) // Can't enqueue while sending
		bro.flags.Set(RECEIVER_ENQUEUING)
	case END_ENQUEUE:
		bro.flags.Unset(RECEIVER_ENQUEUING)
		// This is necesarry because if the session is quiescent (e.g. has no in-flight exchange)
		// the session Run loop will be waiting on RECEIVER_CHECKING, and we need to set it to send
		// an initial 'want' message to the block source. If the receiver is not sending, waiting,
		// or checking, it is quiescant.
		if !bro.flags.ContainsAny(RECEIVER_SENDING | RECEIVER_WAITING | RECEIVER_CHECKING) {
			bro.flags.Set(RECEIVER_CHECKING)
		}
	}

	return nil
}

// State returns the current state of the orchestrator.
func (bro *BatchReceiveOrchestrator) State() BatchState {
	return bro.flags.All()
}

// ReceiveState unsets any current sender state flags, and sets the specified state flags.
func (bro *BatchReceiveOrchestrator) ReceiveState(batchState BatchState) error {
	bro.flags.Update(SENDER, batchState)
	return nil
}

// IsClosed returns true if the receiver is closed.
func (bro *BatchReceiveOrchestrator) IsClosed() bool {
	return bro.flags.Contains(SENDER_CLOSED|RECEIVER_CLOSED) || bro.flags.Contains(CANCELLED)
}
