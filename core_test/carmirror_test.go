package core_test

import (
	"errors"
	"testing"
	"time"

	"math/rand"

	. "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	mock "github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log *zap.SugaredLogger

func init() {
	rand.Seed(time.Now().UnixNano())
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)
	zap.ReplaceGlobals(zap.Must(config.Build()))
	log = zap.S()
	InitDefault()
}

var ErrReceiverNotSet error = errors.New("receiver not set")

func TestAntiEvergreen(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(senderStore, 5, 5, 0.0)

	randomBlock, err := senderStore.RandomBlock()
	if err != nil {
		t.Errorf("Failed to get random block from store")
	}

	if !senderStore.HasAll(root) {
		t.Errorf("Store should have all blocks")
	}

	senderStore.Remove(randomBlock.Id())

	if senderStore.HasAll(root) {
		t.Errorf("Store should not have all blocks")
	}
}

func IdHash(id mock.BlockId, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}

func makeBloom(capacity uint) filter.Filter[mock.BlockId] {
	return filter.NewBloomFilter(capacity, IdHash)
}

type BlockMessage struct {
	status BatchStatus
	blocks []RawBlock[mock.BlockId]
}

type BlockChannel struct {
	channel  chan BlockMessage
	receiver BatchBlockReceiver[mock.BlockId]
}

func (ch *BlockChannel) SendList(status BatchStatus, blocks []RawBlock[mock.BlockId]) error {
	ch.channel <- BlockMessage{status, blocks}
	return nil
}

func (ch *BlockChannel) Close() error {
	close(ch.channel)
	return nil
}

func (ch *BlockChannel) SetBlockListener(receiver BatchBlockReceiver[mock.BlockId]) {
	ch.receiver = receiver
}

func (ch *BlockChannel) listen() error {
	var err error = nil
	for result := range ch.channel {
		if ch.receiver == nil {
			return ErrReceiverNotSet
		}
		ch.receiver.HandleList(result.status, result.blocks)
	}
	return err
}

type MockStatusMessage messages.StatusMessage[mock.BlockId, *mock.BlockId, filter.Filter[mock.BlockId], BatchStatus]

type MockStatusReceiver struct {
	channel        <-chan MockStatusMessage
	statusReceiver StatusReceiver[mock.BlockId, BatchStatus]
}

func (ch *MockStatusReceiver) SetStatusListener(receiver StatusReceiver[mock.BlockId, BatchStatus]) {
	ch.statusReceiver = receiver
}

func (ch *MockStatusReceiver) listen() error {
	var err error = nil
	for result := range ch.channel {
		if ch.statusReceiver == nil {
			return ErrReceiverNotSet
		}
		ch.statusReceiver.HandleStatus(result.Have, result.Want)
		ch.statusReceiver.HandleState(result.Status)
	}
	return err
}

type MockStatusSender struct {
	channel      chan<- MockStatusMessage
	orchestrator Orchestrator[BatchStatus]
}

func NewMockStatusSender(channel chan<- MockStatusMessage, orchestrator Orchestrator[BatchStatus]) *MockStatusSender {
	return &MockStatusSender{
		channel,
		orchestrator,
	}
}

func (sn *MockStatusSender) SendStatus(have filter.Filter[mock.BlockId], want []mock.BlockId) error {
	state := sn.orchestrator.State()
	sn.channel <- MockStatusMessage{state, have, want}
	return nil
}

func (sn *MockStatusSender) Close() error {
	close(sn.channel)
	return nil
}

type MockConnection struct {
	batchBlockChannel BlockChannel
	statusReceiver    MockStatusReceiver
	statusChannel     chan MockStatusMessage
	maxBatchSize      uint
}

func NewMockConnection(maxBatchSize uint) *MockConnection {

	statusChannel := make(chan MockStatusMessage, 1024)

	return &MockConnection{
		BlockChannel{
			make(chan BlockMessage),
			nil,
		},
		MockStatusReceiver{
			statusChannel,
			nil,
		},
		statusChannel,
		maxBatchSize,
	}
}

func (conn *MockConnection) OpenBlockSender(orchestrator Orchestrator[BatchStatus]) BlockSender[mock.BlockId] {
	return NewInstrumentedBlockSender[mock.BlockId](
		NewSimpleBatchBlockSender[mock.BlockId](&conn.batchBlockChannel, orchestrator, uint32(conn.maxBatchSize)),
		GLOBAL_STATS.WithContext("MockBlockSender"),
	)
}

func (conn *MockConnection) OpenStatusSender(orchestrator Orchestrator[BatchStatus]) StatusSender[mock.BlockId] {
	return NewInstrumentedStatusSender[mock.BlockId](
		NewMockStatusSender(conn.statusChannel, orchestrator),
		GLOBAL_STATS.WithContext("MockStatusSender"),
	)
}

func (conn *MockConnection) ListenStatus(sender StatusReceiver[mock.BlockId, BatchStatus]) error {
	conn.statusReceiver.SetStatusListener(sender)
	return conn.statusReceiver.listen()
}

func (conn *MockConnection) ListenBlocks(receiver BlockReceiver[mock.BlockId, BatchStatus]) error {
	conn.batchBlockChannel.SetBlockListener(NewSimpleBatchBlockReceiver(receiver))
	return conn.batchBlockChannel.listen()
}

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

func MockBatchTransfer(sender_store *mock.Store, receiver_store *mock.Store, root mock.BlockId, max_batch_size uint) error {

	snapshotBefore := GLOBAL_REPORTING.Snapshot()
	connection := NewMockConnection(max_batch_size)

	sender_session := NewSenderSession[mock.BlockId, BatchStatus](
		NewInstrumentedBlockStore[mock.BlockId](sender_store, GLOBAL_STATS.WithContext("SenderStore")),
		connection,
		filter.NewSynchronizedFilter(makeBloom(1024)),
		NewInstrumentedOrchestrator[BatchStatus](NewBatchSendOrchestrator(), GLOBAL_STATS.WithContext("BatchSendOrchestrator")),
	)

	log.Debugf("created sender_session")

	receiver_session := NewReceiverSession[mock.BlockId, BatchStatus](
		NewInstrumentedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](receiver_store), GLOBAL_STATS.WithContext("ReceiverStore")),
		connection,
		NewSimpleStatusAccumulator[mock.BlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
		NewInstrumentedOrchestrator[BatchStatus](NewBatchReceiveOrchestrator(), GLOBAL_STATS.WithContext("BatchReceiveOrchestrator")),
	)

	log.Debugf("created receiver_session")

	sender_session.Enqueue(root)
	sender_session.Close()

	log.Debugf("starting goroutines")

	err_chan := make(chan error)
	go func() {
		log.Debugf("sender session started")
		err_chan <- sender_session.Run()
		log.Debugf("sender session terminated")
	}()

	go func() {
		log.Debugf("receiver session started")
		err_chan <- receiver_session.Run()
		log.Debugf("receiver session terminated")
	}()

	go func() {
		log.Debugf("block listener started")
		err_chan <- connection.ListenBlocks(receiver_session)
		log.Debugf("block listener terminated")
	}()

	go func() {
		log.Debugf("status listener started")
		err_chan <- connection.ListenStatus(NewInstrumentedStatusReceiver[mock.BlockId, BatchStatus](sender_session, GLOBAL_STATS.WithContext("StatusListener")))
		log.Debugf("status listener terminated")
	}()

	go func() {
		log.Debugf("timeout started")
		time.Sleep(10 * time.Second)
		log.Debugf("timeout elapsed")
		if !sender_session.IsClosed() {
			sender_session.Cancel()
		}
		if !receiver_session.IsClosed() {
			receiver_session.Cancel()
		}
	}()

	var err error

	for i := 0; i < 4 && err == nil; i++ {
		err = <-err_chan
		log.Debugf("goroutine terminated with %v", err)
	}

	snapshotAfter := GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(log)
	return nil
}

func TestMockTransferToEmptyStoreSingleBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(senderStore, 5, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(senderStore, receiverStore, root, 5000)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(senderStore, 5, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(senderStore, receiverStore, root, 10)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(senderStore, 5, 5, 0.0)
	receiverStore := mock.NewStore()
	receiverStore.AddAll(senderStore)
	block, err := receiverStore.RandomBlock()
	if err != nil {
		t.Errorf("Could not find random block %v", err)
	}
	receiverStore.Remove(block.Id())
	MockBatchTransfer(senderStore, receiverStore, root, 10)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingTreeBlockBatch(t *testing.T) {
	senderStore := mock.NewStore()
	mock.AddRandomForest(senderStore, 10)
	receiverStore := mock.NewStore()
	receiverStore.AddAll(senderStore)
	root := mock.AddRandomTree(senderStore, 12, 5, 0.1)
	MockBatchTransfer(senderStore, receiverStore, root, 10)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}
