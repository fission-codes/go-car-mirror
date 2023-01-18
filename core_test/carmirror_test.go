package core_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"math/rand"

	"github.com/fission-codes/go-car-mirror/batch"
	. "github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/diagrammed"
	instrumented "github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/diagrammer"
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

var testdataDir string = "../testdata"
var stateDiagramsDir string = filepath.Join(testdataDir, "state-diagrams")
var blockStoreConfig mock.Config = mock.Config{
	ReadStorageLatency:   time.Microsecond * 250,
	WriteStorageLatency:  time.Microsecond * 250,
	ReadStorageBandwith:  time.Second / (250 * (1 << 20)), // 250 megabits per second
	WriteStorageBandwith: time.Second / (250 * (1 << 20)), // 250 megabits per second
}

func init() {
	rand.Seed(time.Now().UnixNano())

	stats.InitDefault()

	// Remove the state diagram file
	os.RemoveAll(stateDiagramsDir)
	os.MkdirAll(stateDiagramsDir, 0755)
}

var ErrReceiverNotSet error = errors.New("receiver not set")

func NewTestBatchSinkOrchestrator(diagrammers ...*diagrammer.StateDiagrammer) Orchestrator[BatchState] {
	if len(diagrammers) == 1 {
		return diagrammed.NewDiagrammedBatchSinkOrchestrator(NewBatchSinkOrchestrator(), diagrammers[0])
	} else if len(diagrammers) > 1 {
		panic("too many diagrammers")
	}
	return NewBatchSinkOrchestrator()
}

func NewTestBatchSourceOrchestrator(diagrammers ...*diagrammer.StateDiagrammer) Orchestrator[BatchState] {
	if len(diagrammers) == 1 {
		return diagrammed.NewDiagrammedBatchSourceOrchestrator(NewBatchSourceOrchestrator(), diagrammers[0])
	} else if len(diagrammers) > 1 {
		panic("too many diagrammers")
	}
	return NewBatchSourceOrchestrator()
}

func diagramWriterForTest(t *testing.T) io.WriteCloser {
	fileName := fmt.Sprintf("%s.md", t.Name())
	stateDiagramFileName := path.Join(stateDiagramsDir, fileName)
	stateDiagramsFile, err := os.OpenFile(stateDiagramFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	fmt.Fprintf(stateDiagramsFile, "### %s\n\n", t.Name())
	if err != nil {
		t.Errorf("Failed to open state diagram file: %s", err)
	}
	return stateDiagramsFile
}

func TestAntiEvergreen(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
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
		log.Debugw("received", "object", "BlockChannel", "method", "listen", "state", result.State, "blocks", len(result.Car.Blocks))
		ch.receiver.HandleList(result.State, result.Car.Blocks)
	}
	return err
}

type StatusChannel struct {
	channel  chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]
	receiver *SimpleBatchStatusReceiver[mock.BlockId]
	rate     int64 // number of bytes transmitted per millisecond
	latency  int64 // latency in milliseconds
}

func (ch *StatusChannel) SendStatus(state BatchState, have filter.Filter[mock.BlockId], want []mock.BlockId) error {
	var message *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]
	if ch.rate > 0 || ch.latency > 0 {
		message = messages.NewStatusMessage(state, have, want)
		buf := bytes.Buffer{}
		// Simulated transmission over network
		message.Write(&buf)
		pause := time.Millisecond * time.Duration(int64(buf.Len())/ch.rate+ch.latency)
		log.Debugw("StatusChannel", "pause", pause)
		time.Sleep(pause)
		message.Read(&buf)
	} else {
		message = messages.NewStatusMessage(state, have.Copy(), want)
	}
	ch.channel <- message
	return nil
}

func (ch *StatusChannel) Close() error {
	close(ch.channel)
	return nil
}

func (ch *StatusChannel) SetStatusListener(receiver *SimpleBatchStatusReceiver[mock.BlockId]) {
	ch.receiver = receiver
}

func (ch *StatusChannel) listen() error {
	var err error = nil
	for result := range ch.channel {
		if ch.receiver == nil {
			return ErrReceiverNotSet
		}
		have := result.Have.Any()
		log.Debugw("received", "object", "StatusChannel", "method", "listen", "state", result.State, "have", have.Count(), "want", len(result.Want))
		ch.receiver.HandleStatus(result.State, have, result.Want)
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

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

func MockBatchTransfer(sender_store *mock.Store, receiver_store *mock.Store, root mock.BlockId, sourceOrchestrator Orchestrator[BatchState], sinkOrchestrator Orchestrator[BatchState], max_batch_size uint, bytes_per_ms int64, latency_ms int64) error {

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	blockChannel := BlockChannel{
		make(chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId, BatchState]),
		nil,
		bytes_per_ms,
		latency_ms,
	}

	statusChannel := StatusChannel{
		make(chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]),
		nil,
		bytes_per_ms,
		latency_ms,
	}

	source_connection := batch.NewGenericBatchSourceConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)

	sender_session := source_connection.Session(
		sender_store,
		filter.NewSynchronizedFilter(makeBloom(1024)),
	)

	log.Debugf("created sender_session")

	sink_connection := batch.NewGenericBatchSinkConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)

	receiver_session := sink_connection.Session(
		NewSynchronizedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](receiver_store)),
		NewSimpleStatusAccumulator[mock.BlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
	)

	log.Debugf("created receiver_session")

	blockSender := source_connection.Sender(&blockChannel, uint32(max_batch_size))
	statusSender := sink_connection.Sender(&statusChannel)

	statusChannel.SetStatusListener(source_connection.Receiver(sender_session))
	blockChannel.SetBlockListener(sink_connection.Receiver(receiver_session))

	log.Debugf("created receiver_session")

	sender_session.Enqueue(root)
	sender_session.Close()

	log.Debugf("starting goroutines")

	err_chan := make(chan error)
	go func() {
		log.Debugf("sender session started")
		err_chan <- sender_session.Run(blockSender)
		blockSender.Close()
		log.Debugf("sender session terminated")
	}()

	go func() {
		log.Debugf("receiver session started")
		err_chan <- receiver_session.Run(statusSender)
		statusSender.Close()
		log.Debugf("receiver session terminated")
	}()

	go func() {
		log.Debugf("block listener started")
		err_chan <- blockChannel.listen()
		log.Debugf("block listener terminated")
	}()

	go func() {
		log.Debugf("status listener started")
		err_chan <- statusChannel.listen()
		log.Debugf("status listener terminated")
	}()

	go func() {
		log.Debugf("timeout started")
		time.Sleep(20 * time.Second)
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
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())

	// Set up diagrammers
	w := diagramWriterForTest(t)
	defer w.Close()
	sourceDiagrammer := diagrammer.NewStateDiagrammer("BatchSourceOrchestrator", w)
	defer sourceDiagrammer.Close()
	sinkDiagrammer := diagrammer.NewStateDiagrammer("BatchSinkOrchestrator", w)
	defer sinkDiagrammer.Close()

	sourceOrchestrator := NewTestBatchSourceOrchestrator(sourceDiagrammer)
	sinkOrchestrator := NewTestBatchSinkOrchestrator(sinkDiagrammer)
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 5000, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatch(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 5000, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatch(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 50, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	block, err := receiverStore.RandomBlock()
	if err != nil {
		t.Errorf("Could not find random block %v", err)
	}
	receiverStore.Remove(block.Id())
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 10, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockBatch(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	block, err := receiverStore.RandomBlock()
	if err != nil {
		t.Errorf("Could not find random block %v", err)
	}
	receiverStore.Remove(block.Id())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingTreeBlockBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}

func TestMockTransferSingleMissingTreeBlockBatch(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	sourceOrchestrator := NewTestBatchSourceOrchestrator()
	sinkOrchestrator := NewTestBatchSinkOrchestrator()
	MockBatchTransfer(senderStore, receiverStore, root, sourceOrchestrator, sinkOrchestrator, 50, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}

func TestSessionQuiescence(t *testing.T) {
	if os.Getenv("CM_TEST_QUIESCENCE") == "" {
		t.Skip("skipping quiescence test")
	}

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	blockChannel := BlockChannel{
		make(chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId, BatchState]),
		nil,
		0,
		0,
	}

	statusChannel := StatusChannel{
		make(chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, BatchState]),
		nil,
		0,
		0,
	}

	senderStore := mock.NewStore(mock.DefaultConfig())
	receiverStore := mock.NewStore(mock.DefaultConfig())
	root := mock.RandId()
	mock.NewBlock(root, 100)

	source_connection := batch.NewGenericBatchSourceConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)

	sender_session := source_connection.Session(
		senderStore,
		filter.NewSynchronizedFilter(makeBloom(1024)),
	)

	log.Debugf("created sender_session")

	sink_connection := batch.NewGenericBatchSinkConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)

	receiver_session := sink_connection.Session(
		NewSynchronizedBlockStore[mock.BlockId](receiverStore),
		NewSimpleStatusAccumulator[mock.BlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
	)

	log.Debugf("created receiver_session")

	blockSender := source_connection.Sender(&blockChannel, 100)
	statusSender := sink_connection.Sender(&statusChannel)

	statusChannel.SetStatusListener(source_connection.Receiver(sender_session))
	blockChannel.SetBlockListener(sink_connection.Receiver(receiver_session))

	log.Debugf("created receiver_session")

	sender_session.Enqueue(root)

	log.Debugf("starting goroutines")

	err_chan := make(chan error)
	go func() {
		log.Debugf("sender session started")
		err_chan <- sender_session.Run(blockSender)
		blockSender.Close()
		log.Debugf("sender session terminated")
	}()

	go func() {
		log.Debugf("receiver session started")
		err_chan <- receiver_session.Run(statusSender)
		statusSender.Close()
		log.Debugf("receiver session terminated")
	}()

	go func() {
		log.Debugf("block listener started")
		err_chan <- blockChannel.listen()
		log.Debugf("block listener terminated")
	}()

	go func() {
		log.Debugf("status listener started")
		err_chan <- statusChannel.listen()
		log.Debugf("status listener terminated")
	}()

	go func() {
		log.Debugf("close timeout started")
		time.Sleep(10 * time.Second)
		log.Debugf("close timeout elapsed")
		err_chan <- sender_session.Close()
	}()

	var err error

	for i := 0; i < 5 && err == nil; i++ {
		err = <-err_chan
		if err != nil {
			t.Errorf("goroutine terminated with %v", err)
		} else {
			log.Debugf("goroutine terminated OK")
		}
	}

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)

	numberOfRounds := diff.Count("MockBlockSender.Flush.Ok")
	if numberOfRounds > 2 { // One round to exchange data, one round to close session
		t.Errorf("Expected 2 rounds, actual count %v", numberOfRounds)
	}
}
