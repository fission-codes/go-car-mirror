package core_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"math/rand"

	. "github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/filter"
	mock "github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/fission-codes/go-car-mirror/stats"
	golog "github.com/ipfs/go-log/v2"
	"github.com/zeebo/xxh3"
)

var log = golog.Logger("go-car-mirror")

const TYPICAL_LATENCY = 20
const GBIT_SECOND = (1 << 30) / 8 / 1000 // Gigabit per second -> to bytes per second -> to bytes per millisecond

func init() {
	rand.Seed(time.Now().UnixNano())

	stats.InitDefault()
}

var ErrReceiverNotSet error = errors.New("receiver not set")

func TestAntiEvergreen(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 5, 5, 0.0)

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

type BlockChannel struct {
	channel  chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId, BatchState]
	receiver BatchBlockReceiver[mock.BlockId]
	rate     int64 // number of bytes transmitted per millisecond
	latency  int64 // latency in milliseconds
}

func (ch *BlockChannel) SendList(state BatchState, blocks []RawBlock[mock.BlockId]) error {
	message := messages.NewBlocksMessage(state, blocks)
	if ch.rate > 0 || ch.latency > 0 {
		buf := bytes.Buffer{}
		// Simulated transmission over network
		message.Write(&buf)
		pause := time.Millisecond * time.Duration(int64(buf.Len())/ch.rate+ch.latency)
		log.Debugw("BlockChannel", "pause", pause)
		time.Sleep(pause)
		message.Read(&buf)
	}
	ch.channel <- message
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
		ch.receiver.HandleList(result.State, result.Car.Blocks)
	}
	return err
}

type StatusChannel struct {
	channel  chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]
	receiver StatusReceiver[mock.BlockId, BatchState]
	rate     int64 // number of bytes transmitted per millisecond
	latency  int64 // latency in milliseconds
}

func (ch *StatusChannel) SendStatus(status BatchState, have filter.Filter[mock.BlockId], want []mock.BlockId) error {
	var message *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]
	if ch.rate > 0 || ch.latency > 0 {
		message = messages.NewStatusMessage(status, have, want)
		buf := bytes.Buffer{}
		// Simulated transmission over network
		message.Write(&buf)
		pause := time.Millisecond * time.Duration(int64(buf.Len())/ch.rate+ch.latency)
		log.Debugw("StatusChannel", "pause", pause)
		time.Sleep(pause)
		message.Read(&buf)
	} else {
		message = messages.NewStatusMessage(status, have.Copy(), want)
	}
	ch.channel <- message
	return nil
}

func (ch *StatusChannel) Close() error {
	close(ch.channel)
	return nil
}

func (ch *StatusChannel) SetStatusListener(receiver StatusReceiver[mock.BlockId, BatchState]) {
	ch.receiver = receiver
}

func (ch *StatusChannel) listen() error {
	var err error = nil
	for result := range ch.channel {
		if ch.receiver == nil {
			return ErrReceiverNotSet
		}
		ch.receiver.HandleStatus(result.Have.Any(), result.Want)
		ch.receiver.HandleState(result.State)
	}
	return err
}

type MockStatusSender struct {
	channel      *StatusChannel
	orchestrator Orchestrator[BatchState]
}

func NewMockStatusSender(channel *StatusChannel, orchestrator Orchestrator[BatchState]) *MockStatusSender {
	return &MockStatusSender{
		channel,
		orchestrator,
	}
}

func (sn *MockStatusSender) SendStatus(have filter.Filter[mock.BlockId], want []mock.BlockId) error {
	state := sn.orchestrator.State()
	sn.channel.SendStatus(state, have.Copy(), want)
	return nil
}

func (sn *MockStatusSender) Close() error {
	sn.channel.Close()
	return nil
}

type MockConnection struct {
	batchBlockChannel BlockChannel
	statusChannel     StatusChannel
	maxBatchSize      uint
}

func NewMockConnection(maxBatchSize uint, rate int64, latency int64) *MockConnection {

	return &MockConnection{
		BlockChannel{
			make(chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId, BatchState]),
			nil,
			rate,
			latency,
		},
		StatusChannel{
			make(chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]),
			nil,
			rate,
			latency,
		},
		maxBatchSize,
	}
}

func (conn *MockConnection) OpenBlockSender(orchestrator Orchestrator[BatchState]) BlockSender[mock.BlockId] {
	return stats.NewInstrumentedBlockSender[mock.BlockId](
		NewSimpleBatchBlockSender[mock.BlockId](&conn.batchBlockChannel, orchestrator, uint32(conn.maxBatchSize)),
		stats.GLOBAL_STATS.WithContext("MockBlockSender"),
	)
}

func (conn *MockConnection) OpenStatusSender(orchestrator Orchestrator[BatchState]) StatusSender[mock.BlockId] {
	return stats.NewInstrumentedStatusSender[mock.BlockId](
		NewMockStatusSender(&conn.statusChannel, orchestrator),
		stats.GLOBAL_STATS.WithContext("MockStatusSender"),
	)
}

func (conn *MockConnection) ListenStatus(sender StatusReceiver[mock.BlockId, BatchState]) error {
	conn.statusChannel.SetStatusListener(sender)
	return conn.statusChannel.listen()
}

func (conn *MockConnection) ListenBlocks(receiver BlockReceiver[mock.BlockId, BatchState], orchestrator Orchestrator[BatchState]) error {
	conn.batchBlockChannel.SetBlockListener(NewSimpleBatchBlockReceiver(receiver, orchestrator))
	return conn.batchBlockChannel.listen()
}

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

func MockBatchTransfer(sender_store *mock.Store, receiver_store *mock.Store, root mock.BlockId, max_batch_size uint, bytes_per_ms int64, latency_ms int64) error {

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()
	connection := NewMockConnection(max_batch_size, bytes_per_ms, latency_ms)

	sender_session := NewSourceSession[mock.BlockId, BatchState](
		stats.NewInstrumentedBlockStore[mock.BlockId](sender_store, stats.GLOBAL_STATS.WithContext("SenderStore")),
		filter.NewSynchronizedFilter(makeBloom(1024)),
		stats.NewInstrumentedOrchestrator[BatchState](NewBatchSourceOrchestrator(), stats.GLOBAL_STATS.WithContext("BatchSourceOrchestrator")),
	)

	log.Debugf("created sender_session")

	receiver_orchestrator := stats.NewInstrumentedOrchestrator[BatchState](NewBatchSinkOrchestrator(), stats.GLOBAL_STATS.WithContext("BatchSinkOrchestrator"))

	receiver_session := NewSinkSession[mock.BlockId, BatchState](
		stats.NewInstrumentedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](receiver_store), stats.GLOBAL_STATS.WithContext("ReceiverStore")),
		NewSimpleStatusAccumulator[mock.BlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
		receiver_orchestrator,
	)

	log.Debugf("created receiver_session")

	sender_session.Enqueue(root)
	sender_session.Close()

	log.Debugf("starting goroutines")

	err_chan := make(chan error)
	go func() {
		log.Debugf("sender session started")
		err_chan <- sender_session.Run(connection)
		log.Debugf("sender session terminated")
	}()

	go func() {
		log.Debugf("receiver session started")
		err_chan <- receiver_session.Run(connection)
		log.Debugf("receiver session terminated")
	}()

	go func() {
		log.Debugf("block listener started")
		err_chan <- connection.ListenBlocks(receiver_session, receiver_orchestrator)
		log.Debugf("block listener terminated")
	}()

	go func() {
		log.Debugf("status listener started")
		err_chan <- connection.ListenStatus(stats.NewInstrumentedStatusReceiver[mock.BlockId, BatchState](sender_session, stats.GLOBAL_STATS.WithContext("StatusListener")))
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

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)
	return nil
}

func TestMockTransferToEmptyStoreSingleBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(senderStore, receiverStore, root, 5000, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(senderStore, receiverStore, root, 5000, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(senderStore, receiverStore, root, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(senderStore, receiverStore, root, 50, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	receiverStore.AddAll(context.Background(), senderStore)
	block, err := receiverStore.RandomBlock()
	if err != nil {
		t.Errorf("Could not find random block %v", err)
	}
	receiverStore.Remove(block.Id())
	MockBatchTransfer(senderStore, receiverStore, root, 10, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	receiverStore.AddAll(context.Background(), senderStore)
	block, err := receiverStore.RandomBlock()
	if err != nil {
		t.Errorf("Could not find random block %v", err)
	}
	receiverStore.Remove(block.Id())
	MockBatchTransfer(senderStore, receiverStore, root, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingTreeBlockBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore()
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore()
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	MockBatchTransfer(senderStore, receiverStore, root, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}

func TestMockTransferSingleMissingTreeBlockBatch(t *testing.T) {
	senderStore := mock.NewStore()
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore()
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	MockBatchTransfer(senderStore, receiverStore, root, 50, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}
