package carmirror

import (
	"strings"
	"sync"

	"github.com/fission-codes/go-car-mirror/util"
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
	HandleList(BatchStatus, []Block[I]) error
}

type BatchBlockSender[I BlockId] interface {
	SendList(BatchStatus, []Block[I]) error
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

func (sbbr *SimpleBatchBlockReceiver[I]) HandleList(flags BatchStatus, list []Block[I]) error {
	for _, block := range list {
		sbbr.session.HandleBlock(block)
	}

	sbbr.session.HandleState(flags | RECEIVER_CHECKING)

	return nil
}

type SimpleBatchBlockSender[I BlockId] struct {
	orchestrator Orchestrator[BatchStatus]
	list         []Block[I]
	listMutex    sync.Mutex
	sender       BatchBlockSender[I]
	maxBatchSize uint32
}

func NewSimpleBatchBlockSender[I BlockId](sender BatchBlockSender[I], orchestrator Orchestrator[BatchStatus], maxBatchSize uint32) *SimpleBatchBlockSender[I] {
	return &SimpleBatchBlockSender[I]{
		orchestrator: orchestrator,
		list:         make([]Block[I], 0, maxBatchSize),
		sender:       sender,
		maxBatchSize: maxBatchSize,
	}
}

func (sbbs *SimpleBatchBlockSender[I]) SendBlock(block Block[I]) error {
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

	batchStatus, err := sbbs.orchestrator.GetState()
	if err != nil {
		return err
	}
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
}

func NewBatchSendOrchestrator() *BatchSendOrchestrator {
	return &BatchSendOrchestrator{
		flags: *util.NewSharedFlagSet(BatchStatus(0)),
	}
}

func (bso *BatchSendOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		bso.flags.Set(SENDER_READY)
	case BEGIN_SEND:
		bso.flags.Wait(SENDER_READY)
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
	}

	return nil
}

func (bso *BatchSendOrchestrator) GetState() (BatchStatus, error) {
	return bso.flags.GetAll(), nil
}

func (bso *BatchSendOrchestrator) ReceiveState(batchStatus BatchStatus) error {
	bso.flags.Update(RECEIVER, batchStatus)
	return nil
}

func (bso *BatchSendOrchestrator) IsClosed() (bool, error) {
	return bso.flags.Contains(SENDER_CLOSED), nil
}

// BatchReceiveOrchestrator
type BatchReceiveOrchestrator struct {
	flags util.SharedFlagSet[BatchStatus]
}

func NewBatchReceiveOrchestrator() *BatchReceiveOrchestrator {
	return &BatchReceiveOrchestrator{
		flags: *util.NewSharedFlagSet(BatchStatus(0)),
	}
}

func (bro *BatchReceiveOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		bro.flags.Set(RECEIVER_READY)
	case END_SESSION:
		bro.flags.Update(RECEIVER_READY, RECEIVER_CLOSED)
	case BEGIN_CHECK:
		bro.flags.Wait(RECEIVER_CHECKING)
	case BEGIN_SEND:
		if bro.flags.Contains(SENDER_CLOSING) {
			bro.flags.Set(RECEIVER_CLOSING)
		}
	case END_SEND:
		bro.flags.Unset(RECEIVER_CHECKING)
	}

	return nil
}

func (bro *BatchReceiveOrchestrator) GetState() (BatchStatus, error) {
	return bro.flags.GetAll(), nil
}

func (bro *BatchReceiveOrchestrator) ReceiveState(batchStatus BatchStatus) error {
	// Slight hack here to allow ReceiverChecking to be updated by SimpleBatchBlockReceiver
	bro.flags.Update(SENDER|RECEIVER_CHECKING, batchStatus)
	return nil
}

func (bro *BatchReceiveOrchestrator) IsClosed() (bool, error) {
	return bro.flags.Contains(SENDER_CLOSED), nil
}
