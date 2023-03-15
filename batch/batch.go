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
	SINK_PROCESSING // Sink is processing a batch of blocks
	SINK_FLUSHING   // Sink is in the process of sending a status message
	SINK_WAITING    // Sink is waiting for a batch of blocks
	SINK_ENQUEUEING // Sink is processing a block id that has been explicitly requested
	SINK_SENDING    // Sink may have status pending flush
	SINK_RECEIVING  // Sink is receiving blocks
	SINK_HANDLING_BATCH
	SOURCE_PROCESSING // Source is processing a batch of blocks.  Technically it's also being used to signal that we're *ready* to process, such that BEGIN_PROCESSING will let processing happen.
	SOURCE_FLUSHING   // Source is flushing blocks
	SOURCE_WAITING    // Source is waiting for a status message
	SOURCE_ENQUEUEING // Source is processing a block id that has been explicitly requested
	SOURCE_SENDING    // Source has unflushed blocks
	SOURCE_RECEIVING  // Source is receiving blocks
	SOURCE_HANDLING_BATCH
	SOURCE_CLOSING
	SOURCE_CLOSED
	CANCELLED
	SINK   = SINK_CLOSING | CANCELLED | SINK_PROCESSING | SINK_FLUSHING | SINK_WAITING | SINK_CLOSED | SINK_SENDING | SINK_ENQUEUEING | SINK_RECEIVING | SINK_HANDLING_BATCH
	SOURCE = SOURCE_CLOSING | CANCELLED | SOURCE_PROCESSING | SOURCE_FLUSHING | SOURCE_WAITING | SOURCE_CLOSED | SOURCE_SENDING | SOURCE_ENQUEUEING | SOURCE_RECEIVING | SOURCE_HANDLING_BATCH
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
	if bs&SINK_RECEIVING != 0 {
		strings = append(strings, "SINK_RECEIVING")
	}
	if bs&SINK_WAITING != 0 {
		strings = append(strings, "SINK_WAITING")
	}
	if bs&SINK_ENQUEUEING != 0 {
		strings = append(strings, "SINK_ENQUEUEING")
	}
	if bs&SINK_HANDLING_BATCH != 0 {
		strings = append(strings, "SINK_HANDLING_BATCH")
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
	if bs&SOURCE_RECEIVING != 0 {
		strings = append(strings, "SOURCE_RECEIVING")
	}
	if bs&SOURCE_WAITING != 0 {
		strings = append(strings, "SOURCE_WAITING")
	}
	if bs&SOURCE_FLUSHING != 0 {
		strings = append(strings, "SOURCE_FLUSHING")
	}
	if bs&SOURCE_ENQUEUEING != 0 {
		strings = append(strings, "SOURCE_ENQUEUEING")
	}
	if bs&SOURCE_HANDLING_BATCH != 0 {
		strings = append(strings, "SOURCE_HANDLING_BATCH")
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
	SendList([]core.RawBlock[I]) error
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

	// Possibly notify BEGIN_RECEIVE and END_RECEIVE here.  Check HandleBlock.
	sbbr.orchestrator.Notify(core.BEGIN_RECEIVE)
	defer sbbr.orchestrator.Notify(core.END_RECEIVE)
	sbbr.session.HandleBlocks(list)

	return nil
}

// Orchestrator returns the orchestrator.
func (sbbr *SimpleBatchBlockReceiver[I]) Orchestrator() core.Orchestrator[BatchState] {
	return sbbr.orchestrator
}

// SimpleBatchBlockSender is a simple implementation of BlockSender which wraps a BatchBlockSender
type SimpleBatchBlockSender[I core.BlockId] struct {
	orchestrator      core.Orchestrator[BatchState]
	list              []core.RawBlock[I]
	listMutex         sync.Mutex
	batchBlockSender  BatchBlockSender[I]
	maxBlocksPerRound uint32
}

// NewSimpleBatchBlockSender creates a new SimpleBatchBlockSender.
func NewSimpleBatchBlockSender[I core.BlockId](batchBlockSender BatchBlockSender[I], orchestrator core.Orchestrator[BatchState], maxBlocksPerRound uint32) *SimpleBatchBlockSender[I] {
	return &SimpleBatchBlockSender[I]{
		orchestrator:      orchestrator,
		list:              make([]core.RawBlock[I], 0, maxBlocksPerRound),
		batchBlockSender:  batchBlockSender,
		maxBlocksPerRound: maxBlocksPerRound,
	}
}

// SendBlock adds a block to the batch and sends the batch if it is full.
func (sbbs *SimpleBatchBlockSender[I]) SendBlock(block core.RawBlock[I]) error {
	sbbs.listMutex.Lock()
	sbbs.list = append(sbbs.list, block)
	// listLen := len(sbbs.list)
	sbbs.listMutex.Unlock()

	// TODO: Add logic around maxBatchSizeColdCall if needed.
	// Should this logic be pulled into the main loop?  To ensure we know when we're flushing?
	// if listLen >= int(sbbs.maxBlocksPerRound) {
	// 	return sbbs.Flush()
	// }

	return nil
}

// Close closes the sender.
func (sbbs *SimpleBatchBlockSender[I]) Close() error {
	return sbbs.batchBlockSender.Close()
}

// Flush sends the current batch of blocks.
func (sbbs *SimpleBatchBlockSender[I]) Flush() error {
	if sbbs.Len() > 0 {
		sbbs.listMutex.Lock()
		defer sbbs.listMutex.Unlock()

		// Clone the list before sending, to avoid data races.
		// While we have a mutex in this method, we don't know what will happen when passed to SendList.
		sendList := make([]core.RawBlock[I], len(sbbs.list))
		copy(sendList, sbbs.list)
		log.Debugw("Sending batch", "list length", len(sbbs.list), "copied list length", len(sendList))

		// This is wrapped in begin flush and flush.
		// END_FLUSH is what sets _WAITING.
		// It would be nice to only notify this if we actually have something to flush.
		if err := sbbs.batchBlockSender.SendList(sendList); err != nil {
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

// Orchestrator returns the orchestrator.
func (sbbs *SimpleBatchBlockSender[I]) Orchestrator() core.Orchestrator[BatchState] {
	return sbbs.orchestrator
}

// BatchSourceOrchestrator is an orchestrator for sending batches of blocks.
type BatchSourceOrchestrator struct {
	state     util.SharedFlagSet[BatchState]
	log       *zap.SugaredLogger
	requester bool
}

// NewBatchSourceOrchestrator creates a new BatchSourceOrchestrator.
func NewBatchSourceOrchestrator(requester bool) *BatchSourceOrchestrator {
	return &BatchSourceOrchestrator{
		state:     *util.NewSharedFlagSet(BatchState(0)),
		log:       log.With("component", "BatchSourceOrchestrator"),
		requester: requester,
	}
}

// IsRequester returns true if this is a requester.
func (bso *BatchSourceOrchestrator) IsRequester() bool {
	return bso.requester
}

// Notify notifies the orchestrator of a session event, updating the state as appropriate.
func (bso *BatchSourceOrchestrator) Notify(event core.SessionEvent) error {
	switch event {
	case core.BEGIN_SESSION:
		// This wait allows us to start a session before enqueueing any blocks.
		bso.state.WaitAny(SOURCE_PROCESSING|SOURCE_CLOSED|CANCELLED, 0)
	case core.END_SESSION:
		bso.state.Set(SOURCE_CLOSED)
	case core.BEGIN_ENQUEUE:
		// For sink, we wait for flushing to be 0 and then set enqueueing.
		bso.state.WaitExact(SOURCE_FLUSHING, 0) // Can't enqueue while sending
		bso.state.Set(SOURCE_ENQUEUEING)        // no such state
	case core.END_ENQUEUE:
		bso.state.Update(SOURCE_ENQUEUEING, SOURCE_SENDING)
		bso.state.Set(SOURCE_PROCESSING)
	case core.BEGIN_CLOSE:
		bso.state.Set(SOURCE_CLOSING)
	case core.END_CLOSE:
		// Nothing
	case core.BEGIN_BATCH:
		bso.state.Set(SOURCE_HANDLING_BATCH)
	case core.END_BATCH:
		bso.state.Update(SOURCE_WAITING, SOURCE_PROCESSING)
	case core.BEGIN_PROCESSING:
		// TODO: I think it's possible SOURCE_CLOSED and SOURCE_CLOSING should be removed.  Check.
		state := bso.state.WaitAny(SOURCE_PROCESSING|CANCELLED|SOURCE_CLOSED|SOURCE_CLOSING, 0)
		if state&CANCELLED != 0 {
			bso.log.Errorf("core.Orchestrator waiting for SOURCE_PROCESSING when CANCELLED seen")
			return errors.ErrStateError
		}
	case core.END_PROCESSING:
		// Does nothing
	case core.BEGIN_RECEIVE:
		// Does nothing
		// bso.state.Set(SOURCE_RECEIVING)
	case core.END_RECEIVE:
		// TODO: Maybe unset waiting and wait for end batch to set processing?
		// bso.state.Update(SOURCE_WAITING, SOURCE_PROCESSING)
	case core.BEGIN_FLUSH:
		if bso.requester {
			bso.state.Update(SOURCE_PROCESSING|SOURCE_SENDING, SOURCE_FLUSHING|SOURCE_WAITING)
		} else {
			bso.state.Update(SOURCE_PROCESSING|SOURCE_SENDING, SOURCE_FLUSHING)
		}
	case core.END_FLUSH:
		// bso.state.Update(SOURCE_FLUSHING, SOURCE_PROCESSING)
		bso.state.Unset(SOURCE_FLUSHING)
		if !bso.requester {
			bso.state.Set(SOURCE_PROCESSING)
		}
	case core.BEGIN_SEND:
		bso.state.Set(SOURCE_SENDING)
	case core.END_SEND:
		// Does nothing
	case core.BEGIN_DRAINING:
		bso.state.WaitExact(SOURCE_ENQUEUEING, 0) // Can't drain while enqueueing
		// If we are draining (the queue is empty) and we have no pending blocks to send, we can close
		if bso.state.ContainsExact(SOURCE_CLOSING|SOURCE_SENDING, SOURCE_CLOSING) {
			bso.state.Set(SOURCE_CLOSED)
		}
	case core.END_DRAINING:
		bso.state.Unset(SOURCE_HANDLING_BATCH)
		if bso.state.ContainsExact(SOURCE_CLOSING|SOURCE_SENDING, SOURCE_CLOSING) {
			bso.state.Set(SOURCE_CLOSED)
		}
	case core.CANCEL:
		bso.state.Update(SOURCE, CANCELLED)
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

func (bso *BatchSourceOrchestrator) IsSafeStateToClose() bool {
	// TODO: If we are the requester, this should include SOURCE_WAITING, but we set WAITING even on responder.  Should only set WAITING on requester.
	if bso.requester {
		return !bso.state.ContainsAny(SOURCE_ENQUEUEING | SOURCE_SENDING | SOURCE_FLUSHING | SOURCE_HANDLING_BATCH | SOURCE_WAITING)
	} else {
		return !bso.state.ContainsAny(SOURCE_ENQUEUEING | SOURCE_SENDING | SOURCE_FLUSHING | SOURCE_HANDLING_BATCH)
	}
}

// ShouldFlush returns true if either:
//   - We are requester and we haven't already flushed and are therefore waiting for a response, OR
//   - We are responder and have received a request and are handling it, meaning we haven't responded yet.
func (bso *BatchSourceOrchestrator) ShouldFlush() bool {
	return (bso.IsRequester() && !bso.state.ContainsAny(SOURCE_WAITING)) || bso.state.ContainsAny(SOURCE_HANDLING_BATCH)
}

// BatchSinkOrchestrator is an orchestrator for receiving batches of blocks.
type BatchSinkOrchestrator struct {
	state     util.SharedFlagSet[BatchState]
	log       *zap.SugaredLogger
	requester bool
}

// NewBatchSinkOrchestrator creates a new BatchSinkOrchestrator.
func NewBatchSinkOrchestrator(requester bool) *BatchSinkOrchestrator {
	return &BatchSinkOrchestrator{
		state:     *util.NewSharedFlagSet(BatchState(0)),
		log:       log.With("component", "BatchSinkOrchestrator"),
		requester: requester,
	}
}

// IsRequester returns true if this is a requester.
func (bro *BatchSinkOrchestrator) IsRequester() bool {
	return bro.requester
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
		bro.state.Set(SINK_ENQUEUEING)
	case core.END_ENQUEUE:
		bro.state.Update(SINK_ENQUEUEING, SINK_SENDING)
		if !bro.state.ContainsAny(SINK_FLUSHING | SINK_WAITING | SINK_PROCESSING) {
			bro.state.Set(SINK_PROCESSING)
		}
	case core.BEGIN_CLOSE:
		bro.state.Set(SINK_CLOSING)
	case core.END_CLOSE:
		// TODO: Anything?
	case core.BEGIN_BATCH:
		bro.state.Set(SINK_HANDLING_BATCH)
	case core.END_BATCH:
		bro.state.Update(SINK_WAITING, SINK_PROCESSING)
	case core.BEGIN_PROCESSING:
		// This lets us either proceed or eject at the beginning of every loop iteration.
		// If we add complete session termination check at the top, does this buy us anything?
		state := bro.state.WaitAny(SINK_PROCESSING|CANCELLED|SINK_CLOSING|SINK_CLOSED, 0) // waits for either flag to be set
		if state&CANCELLED != 0 {
			bro.log.Errorf("core.Orchestrator waiting for SINK_PROCESSING when CANCELLED seen")
			return errors.ErrStateError
		}
	case core.END_PROCESSING:
		// if bro.state.ContainsExact(SINK_CLOSING|SINK_SENDING, SINK_CLOSING) {
		// 	bro.state.Set(SINK_CLOSED)
		// }
	case core.BEGIN_RECEIVE:
		// TODO: Anything?
	case core.END_RECEIVE:
		// TODO: Anything?  Source does the following with SOURCE.
		bro.state.Update(SINK_WAITING, SINK_PROCESSING)
	case core.BEGIN_FLUSH:
		if bro.requester {
			bro.state.Update(SINK_PROCESSING|SINK_SENDING, SINK_FLUSHING|SINK_WAITING)
		} else {
			bro.state.Update(SINK_PROCESSING|SINK_SENDING, SINK_FLUSHING)
		}
	case core.END_FLUSH:
		bro.state.Unset(SINK_FLUSHING)
		if !bro.requester {
			bro.state.Set(SINK_PROCESSING)
		}
		// bro.state.Update(SINK_FLUSHING, SINK_PROCESSING)
	case core.BEGIN_SEND:
		bro.state.Set(SINK_SENDING)
	case core.END_SEND:
		// TODO: Anything?
	case core.BEGIN_DRAINING:
		bro.state.WaitExact(SINK_ENQUEUEING, 0) // Can't flush while enqueuing
		// If we are draining (the queue is empty) and we have no pending status to send, we can close
		if bro.state.ContainsExact(SINK_CLOSING|SINK_SENDING, SINK_CLOSING) {
			bro.state.Set(SINK_CLOSED)
		}
	case core.END_DRAINING:
		bro.state.Unset(SINK_SENDING)
		// If we end draining, we will have flushed if needed, and that means we know we've handled any incoming requests, so unset handling batch.
		bro.state.Unset(SINK_HANDLING_BATCH)
		// TODO: Is this needed here and BEGIN_DRAINING?
		if bro.state.ContainsExact(SINK_CLOSING|SINK_SENDING, SINK_CLOSING) {
			bro.state.Set(SINK_CLOSED)
		}
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

func (bro *BatchSinkOrchestrator) IsSafeStateToClose() bool {
	// TODO: If we're the requester, than should include SINK_WAITING.
	if bro.requester {
		return !bro.state.ContainsAny(SINK_ENQUEUEING | SINK_FLUSHING | SINK_HANDLING_BATCH | SINK_WAITING)
	} else {
		return !bro.state.ContainsAny(SINK_ENQUEUEING | SINK_FLUSHING | SINK_HANDLING_BATCH)
	}
}

// ShouldFlush returns true if either:
//   - We are requester and we haven't already flushed and are therefore waiting for a response, OR
//   - We are responder and have received a request and are handling it, meaning we haven't responded yet.
func (bro *BatchSinkOrchestrator) ShouldFlush() bool {
	return (bro.IsRequester() && !bro.state.ContainsAny(SINK_WAITING)) || bro.state.ContainsAny(SINK_HANDLING_BATCH)
}

// TODO: Naming is confusing.  This has BatchStatusReceiver in its name, but it doesn't implement HandleList and therefore isn't a BatchStatusReceiver.
// It is batch only in that it uses the orchestrator with BatchState to notify.
type SimpleBatchStatusReceiver[I core.BlockId] struct {
	statusReceiver core.StatusReceiver[I]
	orchestrator   core.Orchestrator[BatchState]
}

// NewSimpleBatchStatusReceiver creates a new SimpleBatchStatusReceiver.
func NewSimpleBatchStatusReceiver[I core.BlockId](statusReceiver core.StatusReceiver[I], orchestrator core.Orchestrator[BatchState]) *SimpleBatchStatusReceiver[I] {
	return &SimpleBatchStatusReceiver[I]{
		statusReceiver: statusReceiver,
		orchestrator:   orchestrator,
	}
}

// HandleList handles a list of raw blocks.
func (sbsr *SimpleBatchStatusReceiver[I]) HandleStatus(have filter.Filter[I], want []I) error {
	sbsr.orchestrator.Notify(core.BEGIN_BATCH)
	defer sbsr.orchestrator.Notify(core.END_BATCH)

	// TODO: handle errors.  Can't yet because HandleStatus doesn't return an error.
	sbsr.statusReceiver.HandleStatus(have, want)

	return nil
}

// Orchestrator returns the orchestrator.
func (sbsr *SimpleBatchStatusReceiver[I]) Orchestrator() core.Orchestrator[BatchState] {
	return sbsr.orchestrator
}
