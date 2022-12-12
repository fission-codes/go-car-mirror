package carmirror

import (
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/util"
	"go.uber.org/zap"
)

type BatchStatus uint32

const (
	RECEIVER_READY BatchStatus = 1 << iota
	RECEIVER_CLOSING
	RECEIVER_CLOSED
	RECEIVER_CHECKING
	SENDER_READY
	SENDER_CLOSING
	SENDER_CLOSED
	RECEIVER = RECEIVER_READY | RECEIVER_CLOSING | RECEIVER_CHECKING | RECEIVER_CLOSED
	SENDER   = SENDER_READY | SENDER_CLOSING | SENDER_CLOSED
)

func (bs BatchStatus) Strings() []string {
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
	if bs&SENDER_READY != 0 {
		strings = append(strings, "SENDER_READY")
	}
	if bs&SENDER_CLOSING != 0 {
		strings = append(strings, "SENDER_CLOSING")
	}
	if bs&SENDER_CLOSED != 0 {
		strings = append(strings, "SENDER_CLOSED")
	}
	return strings
}

func (bs BatchStatus) String() string {
	return strings.Join(bs.Strings(), "|")
}

type BatchBlockReceiver[I BlockId] interface {
	HandleList(BatchStatus, []RawBlock[I]) error
}

type BatchBlockSender[I BlockId] interface {
	SendList(BatchStatus, []RawBlock[I]) error
	Close() error
}

// ReceiverSession[I BlockId, F Flags]
type SimpleBatchBlockReceiver[I BlockId] struct {
	session BlockReceiver[I, BatchStatus]
}

func NewSimpleBatchBlockReceiver[I BlockId](rs BlockReceiver[I, BatchStatus]) *SimpleBatchBlockReceiver[I] {
	return &SimpleBatchBlockReceiver[I]{
		session: rs,
	}
}

func (sbbr *SimpleBatchBlockReceiver[I]) HandleList(flags BatchStatus, list []RawBlock[I]) error {
	for _, block := range list {
		sbbr.session.HandleBlock(block)
	}

	sbbr.session.HandleState(flags | RECEIVER_CHECKING)

	return nil
}

type SimpleBatchBlockSender[I BlockId] struct {
	orchestrator Orchestrator[BatchStatus]
	list         []RawBlock[I]
	listMutex    sync.Mutex
	sender       BatchBlockSender[I]
	maxBatchSize uint32
}

func NewSimpleBatchBlockSender[I BlockId](sender BatchBlockSender[I], orchestrator Orchestrator[BatchStatus], maxBatchSize uint32) *SimpleBatchBlockSender[I] {
	return &SimpleBatchBlockSender[I]{
		orchestrator: orchestrator,
		list:         make([]RawBlock[I], 0, maxBatchSize),
		sender:       sender,
		maxBatchSize: maxBatchSize,
	}
}

func (sbbs *SimpleBatchBlockSender[I]) SendBlock(block RawBlock[I]) error {
	sbbs.listMutex.Lock()
	sbbs.list = append(sbbs.list, block)
	sbbs.listMutex.Unlock()

	if len(sbbs.list) >= int(sbbs.maxBatchSize) {
		return sbbs.Flush()
	}

	return nil
}

func (sbbs *SimpleBatchBlockSender[I]) Close() error {
	return sbbs.sender.Close()
}

func (sbbs *SimpleBatchBlockSender[I]) Flush() error {
	sbbs.orchestrator.Notify(BEGIN_FLUSH)
	defer sbbs.orchestrator.Notify(END_FLUSH)

	batchStatus := sbbs.orchestrator.State()
	sbbs.listMutex.Lock()
	defer sbbs.listMutex.Unlock()
	if err := sbbs.sender.SendList(batchStatus&SENDER, sbbs.list); err != nil {
		return err
	}
	sbbs.list = sbbs.list[:0]

	return nil
}

// BatchSendOrchestrator
type BatchSendOrchestrator struct {
	flags util.SharedFlagSet[BatchStatus]
	log   *zap.SugaredLogger
}

func NewBatchSendOrchestrator() *BatchSendOrchestrator {
	return &BatchSendOrchestrator{
		flags: *util.NewSharedFlagSet(BatchStatus(0)),
		log:   log.With("component", "BatchSendOrchestrator"),
	}
}

func (bso *BatchSendOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		bso.flags.Set(SENDER_READY)
	case BEGIN_SEND:
		state := bso.flags.WaitAny(SENDER_READY|SENDER_CLOSED, 0)
		if state&SENDER_CLOSED != 0 {
			bso.log.Errorf("Orchestrator waiting for SENDER_READY when SENDER_CLOSED seen")
			return errors.ErrStateError
		}
	case END_RECEIVE:
		bso.flags.Set(SENDER_READY)
	case BEGIN_CLOSE:
		bso.flags.Set(SENDER_CLOSING)
	case BEGIN_FLUSH:
		bso.flags.Unset(SENDER_READY)
	case BEGIN_DRAINING:
		if bso.flags.Contains(RECEIVER_CLOSING) {
			bso.flags.Set(SENDER_CLOSED)
		}
	case END_SESSION:
		bso.flags.Update(SENDER, SENDER_CLOSED)
	case CANCEL:
		bso.flags.Update(SENDER, SENDER_CLOSED)
	}

	return nil
}

func (bso *BatchSendOrchestrator) State() BatchStatus {
	return bso.flags.All()
}

func (bso *BatchSendOrchestrator) ReceiveState(batchStatus BatchStatus) error {
	bso.flags.Update(RECEIVER, batchStatus)
	return nil
}

func (bso *BatchSendOrchestrator) IsClosed() bool {
	return bso.flags.Contains(SENDER_CLOSED)
}

// BatchReceiveOrchestrator
type BatchReceiveOrchestrator struct {
	flags util.SharedFlagSet[BatchStatus]
	log   *zap.SugaredLogger
}

func NewBatchReceiveOrchestrator() *BatchReceiveOrchestrator {
	return &BatchReceiveOrchestrator{
		flags: *util.NewSharedFlagSet(BatchStatus(0)),
		log:   log.With("component", "BatchReceiveOrchestrator"),
	}
}

func (bro *BatchReceiveOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		bro.flags.Set(RECEIVER_READY)
	case END_SESSION:
		bro.flags.Update(RECEIVER_READY, RECEIVER_CLOSED)
	case BEGIN_CHECK:
		state := bro.flags.WaitAny(RECEIVER_CHECKING|RECEIVER_CLOSED, 0)
		if state&RECEIVER_CLOSED != 0 {
			bro.log.Errorf("Orchestrator waiting for RECEIVER_CHECKING when RECEIVER_CLOSED seen")
			return errors.ErrStateError
		}
	case BEGIN_SEND:
		if bro.flags.Contains(SENDER_CLOSING) {
			bro.flags.Set(RECEIVER_CLOSING)
		}
	case END_SEND:
		bro.flags.Unset(RECEIVER_CHECKING)
	case CANCEL:
		bro.flags.Update(RECEIVER, RECEIVER_CLOSED)
	}

	return nil
}

func (bro *BatchReceiveOrchestrator) State() BatchStatus {
	return bro.flags.All()
}

func (bro *BatchReceiveOrchestrator) ReceiveState(batchStatus BatchStatus) error {
	// Slight hack here to allow ReceiverChecking to be updated by SimpleBatchBlockReceiver
	bro.flags.Update(SENDER|RECEIVER_CHECKING, batchStatus)
	return nil
}

func (bro *BatchReceiveOrchestrator) IsClosed() bool {
	return bro.flags.ContainsAny(SENDER_CLOSED | RECEIVER_CLOSED)
}
