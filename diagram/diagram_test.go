package diagram

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"math/rand"

	. "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	mock "github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/zeebo/xxh3"
)

const TYPICAL_LATENCY = 20
const GBIT_SECOND = (1 << 30) / 8 / 1000 // Gigabit per second -> to bytes per second -> to bytes per millisecond

func init() {
	rand.Seed(time.Now().UnixNano())

	InitDefault()
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
	return NewInstrumentedBlockSender[mock.BlockId](
		NewSimpleBatchBlockSender[mock.BlockId](&conn.batchBlockChannel, orchestrator, uint32(conn.maxBatchSize)),
		GLOBAL_STATS.WithContext("MockBlockSender"),
	)
}

func (conn *MockConnection) OpenStatusSender(orchestrator Orchestrator[BatchState]) StatusSender[mock.BlockId] {
	return NewInstrumentedStatusSender[mock.BlockId](
		NewMockStatusSender(&conn.statusChannel, orchestrator),
		GLOBAL_STATS.WithContext("MockStatusSender"),
	)
}

func (conn *MockConnection) ListenStatus(sender StatusReceiver[mock.BlockId, BatchState]) error {
	conn.statusChannel.SetStatusListener(sender)
	return conn.statusChannel.listen()
}

func (conn *MockConnection) ListenBlocks(receiver BlockReceiver[mock.BlockId, BatchState]) error {
	conn.batchBlockChannel.SetBlockListener(NewSimpleBatchBlockReceiver(receiver))
	return conn.batchBlockChannel.listen()
}

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

func MockBatchTransfer(t *testing.T, senderStore *mock.Store, receiverStore *mock.Store, root mock.BlockId, maxBatchSize uint, bytesPerMs int64, latencyMs int64) (string, error) {

	snapshotBefore := GLOBAL_REPORTING.Snapshot()
	connection := NewMockConnection(maxBatchSize, bytesPerMs, latencyMs)

	// TODO: Create a new log and save it to disk, or at least don't output inline with the other logger.
	sendDiagrammer := NewStateDiagrammer("BatchSendOrchestrator", &log.SugaredLogger)
	defer sendDiagrammer.Close()

	senderSession := NewSenderSession[mock.BlockId, BatchState](
		NewInstrumentedBlockStore[mock.BlockId](senderStore, GLOBAL_STATS.WithContext("SenderStore")),
		connection,
		filter.NewSynchronizedFilter(makeBloom(1024)),
		NewDiagrammedBatchSendOrchestrator(NewBatchSendOrchestrator(), sendDiagrammer),
	)

	log.Debugf("created senderSession")

	receiveDiagrammer := NewStateDiagrammer("BatchReceiveOrchestrator", &log.SugaredLogger)
	defer receiveDiagrammer.Close()

	receiverSession := NewReceiverSession[mock.BlockId, BatchState](
		NewInstrumentedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](receiverStore), GLOBAL_STATS.WithContext("ReceiverStore")),
		connection,
		NewSimpleStatusAccumulator[mock.BlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
		NewDiagrammedBatchReceiveOrchestrator(NewBatchReceiveOrchestrator(), receiveDiagrammer),
	)

	log.Debugf("created receiverSession")

	senderSession.Enqueue(root)
	senderSession.Close()

	log.Debugf("starting goroutines")

	errChan := make(chan error)
	go func() {
		log.Debugf("sender session started")
		errChan <- senderSession.Run()
		log.Debugf("sender session terminated")
	}()

	go func() {
		log.Debugf("receiver session started")
		errChan <- receiverSession.Run()
		log.Debugf("receiver session terminated")
	}()

	go func() {
		log.Debugf("block listener started")
		errChan <- connection.ListenBlocks(receiverSession)
		log.Debugf("block listener terminated")
	}()

	go func() {
		log.Debugf("status listener started")
		errChan <- connection.ListenStatus(NewInstrumentedStatusReceiver[mock.BlockId, BatchState](senderSession, GLOBAL_STATS.WithContext("StatusListener")))
		log.Debugf("status listener terminated")
	}()

	go func() {
		log.Debugf("timeout started")
		time.Sleep(10 * time.Second)
		log.Debugf("timeout elapsed")
		if !senderSession.IsClosed() {
			senderSession.Cancel()
		}
		if !receiverSession.IsClosed() {
			receiverSession.Cancel()
		}
	}()

	var err error

	for i := 0; i < 4 && err == nil; i++ {
		err = <-errChan
		log.Debugf("goroutine terminated with %v", err)
	}

	snapshotAfter := GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)

	// Output diagrams
	if err := os.Mkdir("../testdata", 0777); err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
	filename := fmt.Sprintf("../testdata/diagram.state.%s.md", t.Name())
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	sendDiagrammer.Close()
	receiveDiagrammer.Close()
	sendDiagrammer.Write(file)
	receiveDiagrammer.Write(file)
	file.Close()
	filename = strings.Replace(filename, "../", "./", 1)

	return filename, nil
}

func TestMockTransferToEmptyStoreSingleBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 5000, 0, 0)
	fmt.Printf("   Diagram written to %s\n", filename)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 5000, GBIT_SECOND, TYPICAL_LATENCY)
	fmt.Printf("   Diagram written to %s\n", filename)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	MockBatchTransfer(t, senderStore, receiverStore, root, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatch(t *testing.T) {
	senderStore := mock.NewStore()
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore()
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 50, GBIT_SECOND, TYPICAL_LATENCY)
	fmt.Printf("   Diagram written to %s\n", filename)
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
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 10, 0, 0)
	fmt.Printf("   Diagram written to %s\n", filename)
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
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 10, GBIT_SECOND, TYPICAL_LATENCY)
	fmt.Printf("   Diagram written to %s\n", filename)
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
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 50, 0, 0)
	fmt.Printf("   Diagram written to %s\n", filename)
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
	filename, _ := MockBatchTransfer(t, senderStore, receiverStore, root, 50, GBIT_SECOND, TYPICAL_LATENCY)
	fmt.Printf("   Diagram written to %s\n", filename)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}
