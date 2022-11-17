package carmirror

import (
	"sync"

	"github.com/fission-codes/go-car-mirror/util"
)

type BatchStatus uint32

const (
	RECEIVER_READY BatchStatus = iota
	RECEIVER_CLOSING
	RECEIVER_CLOSED
	RECEIVER_CHECKING
	SENDER_READY
	SENDER_CLOSING
	SENDER_CLOSED
	RECEIVER = RECEIVER_READY | RECEIVER_CLOSING | RECEIVER_CHECKING | RECEIVER_CLOSED
	SENDER   = SENDER_READY | SENDER_CLOSING | SENDER_CLOSED
)

type BatchBlockReceiver[I BlockId, B Block[I]] interface {
	HandleList(BatchStatus, B) error
}

type BatchBlockSender[I BlockId, B Block[I]] interface {
	SendList(BatchStatus, []B) error
	Close() error
}

// ReceiverSession[I BlockId, F Flags]
type SimpleBatchBlockReceiver[I BlockId, B Block[I]] struct {
	session ReceiverSession[I, BatchStatus]
}

func NewSimpleBatchBlockReceiver[I BlockId, B Block[I]](rs ReceiverSession[I, BatchStatus]) *SimpleBatchBlockReceiver[I, B] {
	return &SimpleBatchBlockReceiver[I, B]{
		session: rs,
	}
}

func (sbbr *SimpleBatchBlockReceiver[I, B]) HandleList(flags BatchStatus, list []B) error {
	var wg sync.WaitGroup

	for _, block := range list {
		wg.Add(1)
		go func(block B) {
			defer wg.Done()

			sbbr.session.HandleBlock(block)

		}(block)
	}
	wg.Wait()

	sbbr.session.HandleState(flags | RECEIVER_CHECKING)

	return nil
}

type SimpleBatchBlockSender[I BlockId, B Block[I]] struct {
	orchestrator Orchestrator[BatchStatus]
	list         []B
	listMutex    sync.Mutex
	sender       BatchBlockSender[I, B]
	maxBatchSize uint32
}

func NewSimpleBatchBlockSender[I BlockId, B Block[I]](sender BatchBlockSender[I, B], orchestrator Orchestrator[BatchStatus], maxBatchSize uint32) *SimpleBatchBlockSender[I, B] {
	return &SimpleBatchBlockSender[I, B]{
		orchestrator: orchestrator,
		list:         make([]B, 0, maxBatchSize),
		sender:       sender,
		maxBatchSize: maxBatchSize,
	}
}

func (sbbs *SimpleBatchBlockSender[I, B]) SendBlock(block B) error {
	sbbs.listMutex.Lock()
	sbbs.list = append(sbbs.list, block)
	sbbs.listMutex.Unlock()

	if len(sbbs.list) >= int(sbbs.maxBatchSize) {
		return sbbs.Flush()
	}

	return nil
}

func (sbbs *SimpleBatchBlockSender[I, B]) Close() error {
	return sbbs.sender.Close()
}

func (sbbs *SimpleBatchBlockSender[I, B]) Flush() error {
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

func (bso *BatchSendOrchestrator) GetState() BatchStatus {
	return bso.flags.GetAll()
}

func (bso *BatchSendOrchestrator) ReceiveState(batchStatus BatchStatus) {
	bso.flags.Update(RECEIVER, batchStatus)
}

func (bso *BatchSendOrchestrator) IsClosed() bool {
	return bso.flags.Contains(SENDER_CLOSED)
}

// BatchReceiveOrchestrator
type BatchReceiveOrchestrator struct {
	flags util.SharedFlagSet[BatchStatus]
}

func (bro *BatchReceiveOrchestrator) Notify(event SessionEvent) error {
	switch event {
	case BEGIN_SESSION:
		bro.flags.Set(RECEIVER_READY)
	case END_SESSION:
		bro.flags.Update(RECEIVER_READY, RECEIVER_CLOSED)
	case BEGIN_CHECK:
		bro.flags.Wait(RECEIVER_READY)
	case BEGIN_SEND:
		if bro.flags.Contains(SENDER_CLOSING) {
			bro.flags.Set(RECEIVER_CLOSING)
		}
	case END_SEND:
		bro.flags.Unset(RECEIVER_CHECKING)
	}

	return nil
}

func (bro *BatchReceiveOrchestrator) GetState() BatchStatus {
	return bro.flags.GetAll()
}

func (bro *BatchReceiveOrchestrator) ReceiveState(batchStatus BatchStatus) {
	// Slight hack here to allow ReceiverChecking to be updated by SimpleBatchBlockReceiver
	bro.flags.Update(SENDER|RECEIVER_CHECKING, batchStatus)
}

func (bro *BatchReceiveOrchestrator) IsClosed() bool {
	return bro.flags.Contains(SENDER_CLOSED)
}
