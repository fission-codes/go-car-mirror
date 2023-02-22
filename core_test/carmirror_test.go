package core_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"math/rand"

	"github.com/fission-codes/go-car-mirror/batch"
	. "github.com/fission-codes/go-car-mirror/core"
	instrumented "github.com/fission-codes/go-car-mirror/core/instrumented"
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

var blockStoreConfig mock.Config = mock.Config{
	ReadStorageLatency:   time.Microsecond * 250,
	WriteStorageLatency:  time.Microsecond * 250,
	ReadStorageBandwith:  time.Second / (250 * (1 << 20)), // 250 megabits per second
	WriteStorageBandwith: time.Second / (250 * (1 << 20)), // 250 megabits per second
}

func init() {
	rand.Seed(time.Now().UnixNano())

	stats.InitDefault()
}

var ErrReceiverNotSet error = errors.New("receiver not set")

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
	channel  chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId, batch.BatchState]
	receiver batch.BatchBlockReceiver[mock.BlockId]
	rate     int64 // number of bytes transmitted per millisecond
	latency  int64 // latency in milliseconds
}

func (ch *BlockChannel) SendList(state batch.BatchState, blocks []RawBlock[mock.BlockId]) error {
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
	log.Debugw("closing", "object", "BlockChannel")
	close(ch.channel)
	log.Debugw("closed", "object", "BlockChannel")
	return nil
}

func (ch *BlockChannel) SetBlockListener(receiver batch.BatchBlockReceiver[mock.BlockId]) {
	ch.receiver = receiver
}

func (ch *BlockChannel) listen() error {
	var err error = nil
	for result := range ch.channel {
		if ch.receiver == nil {
			return ErrReceiverNotSet
		}
		log.Debugw("received", "object", "BlockChannel", "method", "listen", "state", result.State, "blocks", len(result.Car.Blocks))
		ch.receiver.HandleList(result.Car.Blocks)
	}
	return err
}

type StatusChannel struct {
	channel  chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, batch.BatchState]
	receiver *batch.SimpleBatchStatusReceiver[mock.BlockId]
	rate     int64 // number of bytes transmitted per millisecond
	latency  int64 // latency in milliseconds
}

func (ch *StatusChannel) SendStatus(state batch.BatchState, have filter.Filter[mock.BlockId], want []mock.BlockId) error {
	log.Debugw("sending", "object", "StatusChannel", "method", "SendStatus", "state", state, "have", have, "want", want)
	var message *messages.StatusMessage[mock.BlockId, *mock.BlockId, batch.BatchState]
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
	log.Debugw("closing", "object", "StatusChannel")
	close(ch.channel)
	log.Debugw("closed", "object", "StatusChannel")
	return nil
}

func (ch *StatusChannel) SetStatusListener(receiver *batch.SimpleBatchStatusReceiver[mock.BlockId]) {
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
		// Hung here?  per stack trace pprof?
		ch.receiver.HandleStatus(result.State, have, result.Want)
	}
	return err
}

type MockStatusSender struct {
	channel      *StatusChannel
	orchestrator Orchestrator[batch.BatchState]
}

func NewMockStatusSender(channel *StatusChannel, orchestrator Orchestrator[batch.BatchState]) *MockStatusSender {
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

func MockBatchTransfer(senderStore *mock.Store, receiverStore *mock.Store, root mock.BlockId, maxBatchSize uint, bytesPerMs int64, latencyMs int64) error {

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	blockChannel := BlockChannel{
		make(chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId, batch.BatchState]),
		nil,
		bytesPerMs,
		latencyMs,
	}

	statusChannel := StatusChannel{
		make(chan *messages.StatusMessage[mock.BlockId, *mock.BlockId, batch.BatchState]),
		nil,
		bytesPerMs,
		latencyMs,
	}

	sourceConnection := batch.NewGenericBatchSourceConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)

	senderSession := sourceConnection.Session(
		senderStore,
		filter.NewSynchronizedFilter(makeBloom(1024)),
	)

	log.Debugf("created senderSession")

	sinkConnection := batch.NewGenericBatchSinkConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE)

	receiverSession := sinkConnection.Session(
		NewSynchronizedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](receiverStore)),
		NewSimpleStatusAccumulator[mock.BlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
	)

	log.Debugf("created receiverSession")

	// TODO: Wrap blockChannel in a GenericRequestBatchBlockSender
	// requestBlockChannel := batch.NewGenericRequestBatchBlockSender[mock.BlockId](&blockChannel)
	// blockSender := sourceConnection.Sender(requestBlockChannel, uint32(maxBatchSize))
	blockSender := sourceConnection.Sender(&blockChannel, uint32(maxBatchSize))
	statusSender := sinkConnection.Sender(&statusChannel)

	statusChannel.SetStatusListener(sourceConnection.Receiver(senderSession))
	blockChannel.SetBlockListener(sinkConnection.Receiver(receiverSession))

	log.Debugf("created senders and channels")

	log.Debugf("starting goroutines")

	go func() {
		log.Debugf("block listener started")
		// Will get a value when the channel is closed
		blockChannel.listen()
		log.Debugf("block listener terminated")
	}()

	go func() {
		log.Debugf("status listener started")
		// Will get a value when the channel is closed
		// TODO: hanging here...
		statusChannel.listen()
		log.Debugf("status listener terminated")
	}()

	go func() {
		log.Debugf("before enqueue")
		senderSession.Enqueue(root)
		log.Debugf("after enqueue")

		log.Debugf("sender session started")
		senderSession.Run(blockSender)
	}()

	// Wait for session to start
	<-senderSession.Started()

	go func() {
		log.Debugf("receiver session started")
		receiverSession.Run(statusSender)
	}()

	// Wait for session to start
	<-receiverSession.Started()

	log.Debugf("sessions started")

	go func() {
		log.Debugf("timeout started")
		time.Sleep(20 * time.Second)
		log.Debugf("timeout elapsed")

		// See what goroutine is hung
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		if err := senderSession.Cancel(); err != nil {
			log.Errorf("error cancelling sender session: %v", err)
		}
		if err := receiverSession.Cancel(); err != nil {
			log.Errorf("error cancelling receiver session: %v", err)
		}
	}()

	if err := <-senderSession.Done(); err != nil {
		log.Debugf("sender session failed: %v", err)
	}
	log.Debugf("sender session terminated")

	// TODO: Fix sink session termination and remove the explicit close.
	// receiverSession.Close()

	// Wait for the sessions to close
	if err := <-receiverSession.Done(); err != nil {
		log.Debugf("receiver session failed: %v", err)
	}
	log.Debugf("receiver session terminated")

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)
	return nil
}

func TestMockTransferToEmptyStoreSingleBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())

	MockBatchTransfer(senderStore, receiverStore, root, 5000, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatchDelayed(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransfer(senderStore, receiverStore, root, 5000, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

// TODO: Hanging
func TestMockTransferToEmptyStoreMultiBatchNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	MockBatchTransfer(senderStore, receiverStore, root, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

// TODO: Hanging
func TestMockTransferToEmptyStoreMultiBatchDelayed(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransfer(senderStore, receiverStore, root, 50, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
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

// TODO: Hanging
func TestMockTransferSingleMissingBlockDelayed(t *testing.T) {
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
	MockBatchTransfer(senderStore, receiverStore, root, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingTreeNoDelay(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	MockBatchTransfer(senderStore, receiverStore, root, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}

// TODO: Hanging
func TestMockTransferSingleMissingTreeDelayed(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransfer(senderStore, receiverStore, root, 50, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}
