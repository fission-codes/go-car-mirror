package batch

import (
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
	golog "github.com/ipfs/go-log/v2"
	"go.uber.org/zap"
)

var log = golog.Logger("go-car-mirror")

// BatchState is a bitfield that describes the state of a batch session.
type BatchState uint32

// Named constants for BatchState flags.
const (
	SINK_CLOSING BatchState = 1 << iota
	SINK_CLOSED
	SINK_PROCESSING   // Sink is processing a batch of blocks
	SINK_FLUSHING     // Sink is in the process of sending a status message
	SINK_WAITING      // Sink is waiting for a batch of blocks
	SINK_ENQUEUING    // Sink is processing a block id that has been explicitly requested
	SINK_SENDING      // Sink may have status pending flush
	SOURCE_PROCESSING // Source is processing a batch of blocks.  Technically it's also being used to signal that we're *ready* to process, such that BEGIN_PROCESSING will let processing happen.
	SOURCE_FLUSHING   // Source is flushing blocks
	SOURCE_WAITING    // Source is waiting for a status message
	SOURCE_SENDING    // Source has unflushed blocks
	SOURCE_CLOSING
	SOURCE_CLOSED
	CANCELLED
	SINK   = SINK_CLOSING | CANCELLED | SINK_PROCESSING | SINK_FLUSHING | SINK_WAITING | SINK_CLOSED | SINK_SENDING | SINK_ENQUEUING
	SOURCE = SOURCE_CLOSING | CANCELLED | SOURCE_PROCESSING | SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_CLOSED | SOURCE_SENDING
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
	if bs&SINK_FLUSHING != 0 {
		strings = append(strings, "SINK_FLUSHING")
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
	if bs&SOURCE_SENDING != 0 {
		strings = append(strings, "SOURCE_SENDING")
	}
	if bs&SOURCE_WAITING != 0 {
		strings = append(strings, "SOURCE_WAITING")
	}
	if bs&SOURCE_FLUSHING != 0 {
		strings = append(strings, "SOURCE_FLUSHING")
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
type BatchBlockReceiver[I core.BlockId] interface {
	// HandleList handles a list of blocks.
	HandleList([]core.RawBlock[I]) error
}

// BatchBlockSender is an interface for sending batches of blocks.
type BatchBlockSender[I core.BlockId] interface {
	// SendList sends a list of blocks.
	SendList(BatchState, []core.RawBlock[I]) error
	// Close closes the sender.
	Close() error
}

// SimpleBatchBlockReceiver is a simple implementation of BatchBlockReceiver.
type SimpleBatchBlockReceiver[I core.BlockId] struct {
	session      core.BlockReceiver[I]
	orchestrator core.Orchestrator[BatchState]
}

// NewSimpleBatchBlockReceiver creates a new SimpleBatchBlockReceiver.
func NewSimpleBatchBlockReceiver[I core.BlockId](session core.BlockReceiver[I], orchestrator core.Orchestrator[BatchState]) *SimpleBatchBlockReceiver[I] {
	return &SimpleBatchBlockReceiver[I]{
		session:      session,
		orchestrator: orchestrator,
	}
}

// HandleList handles a list of raw blocks.
func (sbbr *SimpleBatchBlockReceiver[I]) HandleList(list []core.RawBlock[I]) error {
	sbbr.orchestrator.Notify(core.BEGIN_BATCH)
	defer sbbr.orchestrator.Notify(core.END_BATCH)

	for _, block := range list {
		sbbr.session.HandleBlock(block)
	}

	return nil
}

// SimpleBatchBlockSender is a simple implementation of BlockSender which wraps a BatchBlockSender
type SimpleBatchBlockSender[I core.BlockId] struct {
	orchestrator     core.Orchestrator[BatchState]
	list             []core.RawBlock[I]
	listMutex        sync.Mutex
	batchBlockSender BatchBlockSender[I]
	maxBatchSize     uint32
}

// NewSimpleBatchBlockSender creates a new SimpleBatchBlockSender.
func NewSimpleBatchBlockSender[I core.BlockId](batchBlockSender BatchBlockSender[I], orchestrator core.Orchestrator[BatchState], maxBatchSize uint32) *SimpleBatchBlockSender[I] {
	return &SimpleBatchBlockSender[I]{
		orchestrator:     orchestrator,
		list:             make([]core.RawBlock[I], 0, maxBatchSize),
		batchBlockSender: batchBlockSender,
		maxBatchSize:     maxBatchSize,
	}
}

// SendBlock adds a block to the batch and sends the batch if it is full.
func (sbbs *SimpleBatchBlockSender[I]) SendBlock(block core.RawBlock[I]) error {
	sbbs.listMutex.Lock()
	sbbs.list = append(sbbs.list, block)
	listLen := len(sbbs.list)
	sbbs.listMutex.Unlock()

	// TODO: Add logic around maxBatchSizeColdCall if needed.

	if listLen >= int(sbbs.maxBatchSize) {
		return sbbs.Flush()
	}

	return nil
}

// Close closes the sender.
func (sbbs *SimpleBatchBlockSender[I]) Close() error {
	return sbbs.batchBlockSender.Close()
}

// Flush sends the current batch of blocks.
func (sbbs *SimpleBatchBlockSender[I]) Flush() error {
	sbbs.orchestrator.Notify(core.BEGIN_FLUSH)
	defer sbbs.orchestrator.Notify(core.END_FLUSH)

	batchState := sbbs.orchestrator.State()
	// Only actually send a list if we have data OR we are closed, so we would like to communicate this state to the sink
	// TODO: If we're closed we shouldn't be sending anything, right?
	if batchState&(SOURCE_SENDING|SOURCE_CLOSED) != 0 {
		sbbs.listMutex.Lock()
		defer sbbs.listMutex.Unlock()

		// Clone the list before sending, to avoid data races.
		// While we have a mutex in this method, we don't know what will happen when passed to SendList.
		sendList := make([]core.RawBlock[I], len(sbbs.list))
		copy(sendList, sbbs.list)

		if err := sbbs.batchBlockSender.SendList(batchState&SOURCE, sendList); err != nil {
			return err
		}
		sbbs.list = sbbs.list[:0]
	}
	return nil
}

// Len returns the number of blocks waiting to be sent.
func (sbbs *SimpleBatchBlockSender[I]) Len() int {
	sbbs.listMutex.Lock()
	defer sbbs.listMutex.Unlock()
	return len(sbbs.list)
}

// BatchSourceOrchestrator is an orchestrator for sending batches of blocks.
type BatchSourceOrchestrator struct {
	state util.SharedFlagSet[BatchState]
	log   *zap.SugaredLogger
}

// NewBatchSourceOrchestrator creates a new BatchSourceOrchestrator.
func NewBatchSourceOrchestrator() *BatchSourceOrchestrator {
	return &BatchSourceOrchestrator{
		state: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchSourceOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event.
// Events lead to state transitions in the orchestrator.
// TODO: Should context be passed, to allow for cancellation and timeouts?  In particular I'm thinking of ENQUEUE or any function that could impact user experience.
func (bso *BatchSourceOrchestrator) Notify(event core.SessionEvent) error {
	switch event {
	case core.BEGIN_SESSION:
		// This wait allows us to start a session before enqueueing any blocks.
		bso.state.WaitAny(SOURCE_PROCESSING|CANCELLED|SOURCE_CLOSED|SOURCE_CLOSING, 0)
	case core.END_SESSION:
		bso.state.Update(SOURCE, SOURCE_CLOSED)
	case core.BEGIN_ENQUEUE:
		// Does nothing
	case core.END_ENQUEUE:
		// This is necessary because if the session is quiescent (e.g. has no in-flight exchange)
		// the session Run loop will be waiting on SOURCE_PROCESSING, and we need to set it to wake
		// up the session.  If the receiver is not flushing, waiting, or ready, it is quiescent.
		// TODO: This shouldn't be needed any more, since there's no longer such thing as a quiescent session,
		// and we terminate the loop based on session termination criteria before notifying BEGIN_PROCESSING.
		// Didn't turn out to be true.  Hung.  But I should try again and see why and what I'm missing.
		if !bso.state.ContainsAny(SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_PROCESSING) {
			bso.state.Set(SOURCE_PROCESSING)
		}
	case core.BEGIN_PROCESSING:
		// TODO: I think it's possible SOURCE_CLOSED and SOURCE_CLOSING should be removed.  Check.
		state := bso.state.WaitAny(SOURCE_PROCESSING|CANCELLED|SOURCE_CLOSED|SOURCE_CLOSING, 0)
		if state&CANCELLED != 0 {
			bso.log.Errorf("core.Orchestrator waiting for SOURCE_PROCESSING when CANCELLED seen")
			return errors.ErrStateError
		}
	case core.END_RECEIVE:
		bso.state.Update(SOURCE_WAITING, SOURCE_PROCESSING)
	case core.BEGIN_CLOSE:
		if bso.state.Contains(SOURCE_WAITING) {
			bso.state.Update(SOURCE_WAITING, SOURCE_CLOSED)
		} else {
			bso.state.Set(SOURCE_CLOSING)
		}
	case core.BEGIN_FLUSH:
		bso.state.Update(SOURCE_PROCESSING, SOURCE_FLUSHING)
	case core.END_FLUSH:
		bso.state.Update(SOURCE_FLUSHING|SOURCE_SENDING, SOURCE_WAITING)
	case core.BEGIN_DRAINING:
		// If we are draining (the queue is empty) and we have no pending blocks to send, we can close
		if bso.state.ContainsExact(SOURCE_CLOSING|SOURCE_SENDING, SOURCE_CLOSING) {
			bso.state.Set(SOURCE_CLOSED)
		}
	case core.END_DRAINING:
		if bso.state.ContainsExact(SOURCE_CLOSING|SOURCE_SENDING, SOURCE_CLOSING) {
			bso.state.Set(SOURCE_CLOSED)
		}
	case core.CANCEL:
		bso.state.Update(SOURCE, CANCELLED)
	case core.BEGIN_SEND:
		bso.state.Set(SOURCE_SENDING)
	}

	return nil
}

// State returns the current state of the orchestrator.
func (bso *BatchSourceOrchestrator) State() BatchState {
	return bso.state.All()
}

// IsClosed returns true if the sender is closed.
func (bso *BatchSourceOrchestrator) IsClosed() bool {
	return bso.state.ContainsAny(SOURCE_CLOSED | CANCELLED)
}

func (bso *BatchSourceOrchestrator) ShouldClose() bool {
	// TODO: Not needed for source orchestrator?
	return false
}

// BatchSinkOrchestrator is an orchestrator for receiving batches of blocks.
type BatchSinkOrchestrator struct {
	state util.SharedFlagSet[BatchState]
	log   *zap.SugaredLogger
}

// NewBatchSinkOrchestrator creates a new BatchSinkOrchestrator.
func NewBatchSinkOrchestrator() *BatchSinkOrchestrator {
	return &BatchSinkOrchestrator{
		state: *util.NewSharedFlagSet(BatchState(0)),
		log:   log.With("component", "BatchSinkOrchestrator"),
	}
}

// Notify notifies the orchestrator of a session event, updating the state as appropriate.
func (bro *BatchSinkOrchestrator) Notify(event core.SessionEvent) error {
	// TODO: at this point we probably need to enclose all this in a mutex.
	switch event {
	case core.BEGIN_SESSION:
		// This wait allows us to start a session before handling any status messages.
		bro.state.WaitAny(SINK_PROCESSING|SINK_CLOSED|CANCELLED, 0)
	case core.END_SESSION:
		bro.state.Set(SINK_CLOSED)
	case core.BEGIN_ENQUEUE:
		bro.state.WaitExact(SINK_FLUSHING, 0) // Can't enqueue while sending
		bro.state.Set(SINK_ENQUEUING)
	case core.END_ENQUEUE:
		bro.state.Update(SINK_ENQUEUING, SINK_SENDING)
		bro.state.Set(SINK_PROCESSING)
		// // This is necessary because if the session is quiescent (e.g. has no in-flight exchange)
		// // the session Run loop will be waiting on SINK_PROCESSING, and we need to set it to send
		// // an initial 'want' message to the block source. If the receiver is not sending, waiting,
		// // or checking, it is quiescent.
		// if !bro.state.ContainsAny(SINK_FLUSHING | SINK_WAITING | SINK_PROCESSING) {
		// 	bro.state.Set(SINK_PROCESSING)
		// }
	case core.BEGIN_CLOSE:
		bro.state.Set(SINK_CLOSING)
	case core.END_CLOSE:
		// TODO: Anything?
	case core.BEGIN_PROCESSING:
		// This lets us either proceed or eject at the beginning of every loop iteration.
		// If we add complete session termination check at the top, does this buy us anything?
		state := bro.state.WaitAny(SINK_PROCESSING|CANCELLED, 0) // waits for either flag to be set
		if state&CANCELLED != 0 {
			bro.log.Errorf("core.Orchestrator waiting for SINK_PROCESSING when CANCELLED seen")
			return errors.ErrStateError
		}
	case core.END_PROCESSING:
		// TODO: Anything?
	case core.BEGIN_RECEIVE:
		// TODO: Anything?
	case core.END_RECEIVE:
		// TODO: Anything?  Source does the following with SOURCE.
		// bro.state.Update(SINK_WAITING, SINK_PROCESSING)
	case core.BEGIN_SEND:
		// bro.state.Set(SINK_SENDING)
	case core.END_SEND:
		// TODO: Anything?
	case core.BEGIN_DRAINING:
		bro.state.WaitExact(SINK_ENQUEUING, 0) // Can't flush while enqueuing
		bro.state.Update(SINK_PROCESSING, SINK_FLUSHING)
		// If we are draining (the queue is empty) and we have no pending status to send, we can close
		if bro.state.ContainsExact(SINK_CLOSING|SINK_SENDING, SINK_CLOSING) {
			bro.state.Set(SINK_CLOSED)
		}
	case core.END_DRAINING:
		bro.state.Update(SINK_FLUSHING|SINK_SENDING, SINK_WAITING)

		// TODO: Is this needed here and BEGIN_DRAINING?
		if bro.state.ContainsExact(SINK_CLOSING|SINK_SENDING, SINK_CLOSING) {
			bro.state.Set(SINK_CLOSED)
		}
	case core.BEGIN_BATCH:
		// TODO: Anything?
	case core.END_BATCH:
		bro.state.Update(SINK_WAITING, SINK_PROCESSING)
	case core.CANCEL:
		bro.state.Update(SINK, CANCELLED)
	}

	return nil
}

// State returns the current state of the orchestrator.
func (bro *BatchSinkOrchestrator) State() BatchState {
	return bro.state.All()
}

// IsClosed returns true if the receiver is closed.
func (bro *BatchSinkOrchestrator) IsClosed() bool {
	return bro.state.ContainsAny(SINK_CLOSED | CANCELLED)
}

func (bro *BatchSinkOrchestrator) ShouldClose() bool {
	return !bro.state.ContainsAny(SINK_ENQUEUING | SINK_SENDING | SINK_FLUSHING)
}

type SimpleBatchStatusReceiver[I core.BlockId] struct {
	session      core.StatusReceiver[I]
	orchestrator core.Orchestrator[BatchState]
}

// NewSimpleBatchStatusReceiver creates a new SimpleBatchStatusReceiver.
func NewSimpleBatchStatusReceiver[I core.BlockId](session core.StatusReceiver[I], orchestrator core.Orchestrator[BatchState]) *SimpleBatchStatusReceiver[I] {
	return &SimpleBatchStatusReceiver[I]{
		session:      session,
		orchestrator: orchestrator,
	}
}

// HandleList handles a list of raw blocks.
func (sbbr *SimpleBatchStatusReceiver[I]) HandleStatus(state BatchState, have filter.Filter[I], want []I) error {
	sbbr.orchestrator.Notify(core.BEGIN_BATCH)
	defer sbbr.orchestrator.Notify(core.END_BATCH)
	// TODO: handle errors.  Can't yet because HandleStatus doesn't return an error.
	sbbr.session.HandleStatus(have, want)

	return nil
}
