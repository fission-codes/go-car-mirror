package core

import (
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
	"go.uber.org/zap"
)

// BatchState is a bitfield that describes the state of a batch session.
type BatchState uint32

// Named constants for BatchState flags.
const (
	SINK_CLOSING BatchState = 1 << iota
	SINK_CLOSED
	SINK_PROCESSING   // Sink is processing a batch of blocks
	SINK_SENDING      // Sink is in the process of sending a status message
	SINK_WAITING      // Sink is waiting for a batch of blocks
	SINK_ENQUEUING    // Sink is processing a block id that has been explicitly requested
	SOURCE_PROCESSING // Source is processing a batch of blocks
	SOURCE_FLUSHING   // Source is flushing blocks
	SOURCE_WAITING    // Source is waiting for a status message
	SOURCE_CLOSING
	SOURCE_CLOSED
	CANCELLED
	SINK   = CANCELLED | SINK_CLOSING | SINK_PROCESSING | SINK_CLOSED | SINK_SENDING | SINK_ENQUEUING | SINK_WAITING
	SOURCE = CANCELLED | SOURCE_PROCESSING | SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_CLOSING | SOURCE_CLOSED
)

// Strings returns a slice of strings describing the given BatchState.
func (bs BatchState) Strings() []string {
	var strings []string
	if bs&SINK_CLOSING != 0 {
		strings = append(strings, "SINK_CLOSING")
	}
	if bs&SINK_CLOSED != 0 {
		strings = append(strings, "SINK_CLOSED")
	}
	if bs&SINK_PROCESSING != 0 {
		strings = append(strings, "SINK_PROCESSING")
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
	if bs&SOURCE_PROCESSING != 0 {
		strings = append(strings, "SOURCE_PROCESSING")
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
	sbbr.orchestrator.ReceiveState(flags)
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

// NewBatchSourceOrchestrator creates a new BatchSourceOrchestrator.
func NewBatchSourceOrchestrator() *BatchSourceOrchestrator {
	return &BatchSourceOrchestrator{
		flags: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchSourceOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event.
// Events lead to state transitions in the orchestrator.
//
func (bso *BatchSourceOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SEND:
		state := bso.flags.WaitAny(SOURCE_PROCESSING|CANCELLED, 0)
		if state&CANCELLED != 0 {
			bso.log.Errorf("Orchestrator waiting for SOURCE_PROCESSING when CANCELLED seen")
			return errors.ErrStateError
		}
	case END_RECEIVE:
		bso.flags.Update(SOURCE_WAITING, SOURCE_PROCESSING)
	case BEGIN_CLOSE:
		bso.flags.Set(SOURCE_CLOSING)
	case BEGIN_FLUSH:
		bso.flags.Update(SOURCE_PROCESSING, SOURCE_FLUSHING)
	case END_FLUSH:
		bso.flags.Update(SOURCE_FLUSHING, SOURCE_WAITING)
	case BEGIN_DRAINING:
		if bso.flags.ContainsAny(SINK_CLOSING | SOURCE_CLOSING) {
			bso.flags.Set(SOURCE_CLOSED)
		}
	case END_ENQUEUE:
		// This is necessary because if the session is quiescent (e.g. has no in-flight exchange)
		// the session Run loop will be waiting on SOURCE_PROCESSING, and we need to set it to wake
		// up the session.  If the receiver is not flushing, waiting, or ready, it is quiescent.
		if !bso.flags.ContainsAny(SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_PROCESSING) {
			bso.flags.Set(SOURCE_PROCESSING)
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

// NewBatchSinkOrchestrator creates a new BatchSinkOrchestrator.
func NewBatchSinkOrchestrator() *BatchSinkOrchestrator {
	return &BatchSinkOrchestrator{
		flags: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchSinkOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event, updating the state as appropriate.
func (bro *BatchSinkOrchestrator) Notify(event SessionEvent) error {
	// TODO: at this point we probably need to enclose all this in a mutex.
	switch event {
	case END_SESSION:
		bro.flags.Set(SINK_CLOSED)
	case BEGIN_CLOSE:
		bro.flags.Set(SINK_CLOSING)
	case BEGIN_CHECK:
		state := bro.flags.WaitAny(SINK_PROCESSING|CANCELLED, 0) // waits for either flag to be set
		if state&CANCELLED != 0 {
			bro.log.Errorf("Orchestrator waiting for SINK_PROCESSING when CANCELLED seen")
			return errors.ErrStateError
		}
	case BEGIN_SEND:
		bro.flags.WaitExact(SINK_ENQUEUING, 0) // Can't send while enqueuing
		bro.flags.Update(SINK_PROCESSING, SINK_SENDING)
		if bro.flags.Contains(SOURCE_CLOSED) {
			bro.flags.Set(SINK_CLOSED)
		}
	case END_BATCH:
		bro.flags.Update(SINK_WAITING, SINK_PROCESSING)
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
		// the session Run loop will be waiting on SINK_PROCESSING, and we need to set it to send
		// an initial 'want' message to the block source. If the receiver is not sending, waiting,
		// or checking, it is quiescent.
		if !bro.flags.ContainsAny(SINK_SENDING | SINK_WAITING | SINK_PROCESSING) {
			bro.flags.Set(SINK_PROCESSING)
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

type SimpleBatchStatusReceiver[I BlockId] struct {
	session      StatusReceiver[I, BatchState]
	orchestrator Orchestrator[BatchState]
}

// NewSimpleBatchStatusReceiver creates a new SimpleBatchStatusReceiver.
func NewSimpleBatchStatusReceiver[I BlockId](rs StatusReceiver[I, BatchState], orchestrator Orchestrator[BatchState]) *SimpleBatchStatusReceiver[I] {
	return &SimpleBatchStatusReceiver[I]{
		session:      rs,
		orchestrator: orchestrator,
	}
}

// HandleList handles a list of raw blocks.
func (sbbr *SimpleBatchStatusReceiver[I]) HandleStatus(flags BatchState, have filter.Filter[I], want []I) error {
	sbbr.session.HandleStatus(have, want)
	return sbbr.orchestrator.ReceiveState(flags)
}
