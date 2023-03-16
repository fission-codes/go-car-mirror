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

const MOCK_ID_HASH = 2
const SESSION_ID = "hardcoded-session-id"

func init() {
	rand.Seed(time.Now().UnixNano())

	filter.RegisterHash(MOCK_ID_HASH, mock.XX3HashBlockId)
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

type BlockChannel struct {
	channel          chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId]
	receiverFunc     func() batch.BatchBlockReceiver[mock.BlockId]
	rate             int64 // number of bytes transmitted per millisecond
	latency          int64 // latency in milliseconds
	orchestratorFunc func(*BlockChannel) Orchestrator[batch.BatchState]
}

func (ch *BlockChannel) SendList(blocks []RawBlock[mock.BlockId]) error {
	if err := ch.orchestratorFunc(ch).Notify(BEGIN_FLUSH); err != nil {
		return err
	}
	message := messages.NewBlocksMessage(blocks)
	if ch.rate > 0 || ch.latency > 0 {
		buf := bytes.Buffer{}
		// Simulated transmission over network
		message.Write(&buf)
		pause := time.Millisecond * time.Duration(int64(buf.Len())/ch.rate+ch.latency)
		log.Debugw("BlockChannel", "pause", pause)
		time.Sleep(pause)
		message.Read(&buf)
	}
	log.Debugw("sending", "object", "BlockChannel", "method", "SendList", "blocks", len(blocks))
	ch.channel <- message
	log.Debugw("sent", "object", "BlockChannel", "method", "SendList", "blocks", len(blocks))

	if err := ch.orchestratorFunc(ch).Notify(END_FLUSH); err != nil {
		return err
	}
	return nil
}

func (ch *BlockChannel) Close() error {
	log.Debugw("closing", "object", "BlockChannel")
	close(ch.channel)
	log.Debugw("closed", "object", "BlockChannel")
	return nil
}

func (ch *BlockChannel) Receiver() batch.BatchBlockReceiver[mock.BlockId] {
	return ch.receiverFunc()
}

func (ch *BlockChannel) SetReceiverFunc(receiverFunc func() batch.BatchBlockReceiver[mock.BlockId]) {
	ch.receiverFunc = receiverFunc
}

func (ch *BlockChannel) SetOrchestratorFunc(orchestratorFunc func(*BlockChannel) Orchestrator[batch.BatchState]) {
	ch.orchestratorFunc = orchestratorFunc
}

func (ch *BlockChannel) listen() error {
	var err error = nil
	for result := range ch.channel {
		receiver := ch.Receiver()
		if receiver == nil {
			return ErrReceiverNotSet
		}
		log.Debugw("received", "object", "BlockChannel", "method", "listen", "blocks", len(result.Car.Blocks))
		err := receiver.HandleList(result.Car.Blocks)
		log.Debugw("handled list", "object", "BlockChannel", "method", "listen", "blocks", len(result.Car.Blocks), "err", err)
	}
	return err
}

type StatusChannel struct {
	channel          chan *messages.StatusMessage[mock.BlockId, *mock.BlockId]
	receiverFunc     func() *batch.SimpleBatchStatusReceiver[mock.BlockId]
	rate             int64 // number of bytes transmitted per millisecond
	latency          int64 // latency in milliseconds
	orchestratorFunc func(*StatusChannel) Orchestrator[batch.BatchState]
}

func (ch *StatusChannel) SendStatus(have filter.Filter[mock.BlockId], want []mock.BlockId) error {
	if err := ch.orchestratorFunc(ch).Notify(BEGIN_FLUSH); err != nil {
		return err
	}
	var message *messages.StatusMessage[mock.BlockId, *mock.BlockId]
	if ch.rate > 0 || ch.latency > 0 {
		message = messages.NewStatusMessage(have.Copy(), want)
		buf := bytes.Buffer{}
		// Simulated transmission over network
		message.Write(&buf)
		pause := time.Millisecond * time.Duration(int64(buf.Len())/ch.rate+ch.latency)
		log.Debugw("StatusChannel", "pause", pause)
		time.Sleep(pause)
		message.Read(&buf)
	} else {
		message = messages.NewStatusMessage(have.Copy(), want)
	}
	log.Debugw("sending", "object", "StatusChannel", "method", "SendStatus", "message", message)
	ch.channel <- message
	log.Debugw("sent", "object", "StatusChannel", "method", "SendStatus", "message", message)
	if err := ch.orchestratorFunc(ch).Notify(END_FLUSH); err != nil {
		return err
	}
	return nil
}

func (ch *StatusChannel) Close() error {
	log.Debugw("closing", "object", "StatusChannel")
	close(ch.channel)
	log.Debugw("closed", "object", "StatusChannel")
	return nil
}

func (ch *StatusChannel) Receiver() *batch.SimpleBatchStatusReceiver[mock.BlockId] {
	return ch.receiverFunc()
}

func (ch *StatusChannel) SetReceiverFunc(receiverFunc func() *batch.SimpleBatchStatusReceiver[mock.BlockId]) {
	ch.receiverFunc = receiverFunc
}

func (ch *StatusChannel) SetOrchestratorFunc(orchestratorFunc func(*StatusChannel) Orchestrator[batch.BatchState]) {
	ch.orchestratorFunc = orchestratorFunc
}

func (ch *StatusChannel) listen() error {
	var err error = nil
	for result := range ch.channel {
		receiver := ch.Receiver()
		if receiver == nil {
			return ErrReceiverNotSet
		}
		have := result.Have.Any()
		log.Debugw("received", "object", "StatusChannel", "method", "listen", "have", have.Count(), "want", len(result.Want))
		receiver.HandleStatus(have, result.Want)
	}
	return err
}

// TODO: MockStatusSender isn't currently used anywhere.
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
	if err := sn.orchestrator.Notify(BEGIN_FLUSH); err != nil {
		return err
	}
	sn.channel.SendStatus(have.Copy(), want)
	if err := sn.orchestrator.Notify(END_FLUSH); err != nil {
		return err
	}
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

// http tests did this, but it is right?

func MockBatchTransferSend(senderStore *mock.Store, receiverStore *mock.Store, root mock.BlockId, maxBlocksPerRound uint32, maxBlocksPerColdCall uint32, bytesPerMs int64, latencyMs int64) error {

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	config := batch.Config{
		MaxBlocksPerRound:    maxBlocksPerRound,
		MaxBlocksPerColdCall: maxBlocksPerColdCall,
		BloomCapacity:        1024,
		BloomFunction:        MOCK_ID_HASH,
		Instrument:           instrumented.INSTRUMENT_STORE | instrumented.INSTRUMENT_ORCHESTRATOR,
	}

	sourceConnection := batch.NewGenericBatchSourceConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE, maxBlocksPerRound, true) // Requester

	senderSession := sourceConnection.Session(
		senderStore,
		filter.NewSynchronizedFilter(batch.NewBloomAllocator[mock.BlockId](&config)()),
		true, // Requester
	)

	sinkResponder := batch.NewSinkResponder[mock.BlockId](receiverStore, config, nil)

	blockChannel := BlockChannel{
		make(chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId]),
		nil,
		bytesPerMs,
		latencyMs,
		func(*BlockChannel) Orchestrator[batch.BatchState] { return sourceConnection },
	}

	blockChannel.SetReceiverFunc(func() batch.BatchBlockReceiver[mock.BlockId] {
		// This will start up a new sink session whenever needed, like if the previously running session's Run method already returned.
		return sinkResponder.Receiver(SESSION_ID)
	})

	statusChannel := StatusChannel{
		make(chan *messages.StatusMessage[mock.BlockId, *mock.BlockId]),
		nil,
		bytesPerMs,
		latencyMs,
		func(*StatusChannel) Orchestrator[batch.BatchState] { return sinkResponder.SinkConnection(SESSION_ID) },
	}

	statusChannel.SetReceiverFunc(func() *batch.SimpleBatchStatusReceiver[mock.BlockId] {
		return sourceConnection.Receiver(senderSession)
	})

	blockSender := sourceConnection.Sender(&blockChannel, uint32(maxBlocksPerRound))

	sinkResponder.SetStatusSender(&statusChannel)

	log.Debugf("created senders and channels")

	log.Debugf("starting goroutines")

	go func() {
		log.Debugf("block listener started")
		// Will get a value when the channel is closed

		err := blockChannel.listen()
		if err != nil {
			log.Debugf("block listener error: %s", err)
		} else {
			log.Debugf("block listener terminated")
		}
	}()

	go func() {
		log.Debugf("status listener started")
		err := statusChannel.listen()
		if err != nil {
			log.Debugf("status listener error: %s", err)
		} else {
			log.Debugf("status listener terminated")
		}
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
		log.Debugf("timeout started")
		time.Sleep(30 * time.Second)
		log.Debugf("timeout elapsed")

		// If we timeout, see what goroutines are hung
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		if err := senderSession.Cancel(); err != nil {
			log.Errorf("error cancelling sender session: %v", err)
		}
		if err := sinkResponder.SinkSession(SESSION_ID).Cancel(); err != nil {
			log.Errorf("error cancelling receiver session: %v", err)
		}
	}()

	if err := <-senderSession.Done(); err != nil {
		log.Debugf("sender session failed: %v", err)
	}
	log.Debugf("sender session terminated")

	if err := blockSender.Close(); err != nil {
		log.Errorf("error closing block sender: %v", err)
	}

	// TODO: Replace with waiting for all channels on the sinkResponder's sessions to be done
	// If we get here, the sender session is done, so this will get the last living sink session's status

	// if err := <-sinkResponder.SinkSession(SESSION_ID).Done(); err != nil {
	// 	log.Debugf("receiver session failed: %v", err)
	// }
	// log.Debugf("receiver session terminated")

	log.Debugw("waiting for all done", "sessions", len(sinkResponder.SinkSessionIds()))
	sinkResponder.AllDone()

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)
	return nil
}

func MockBatchTransferReceive(sinkStore *mock.Store, sourceStore *mock.Store, root mock.BlockId, maxBlocksPerRound uint32, bytesPerMs int64, latencyMs int64) error {

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	config := batch.Config{
		MaxBlocksPerRound: maxBlocksPerRound,
		BloomCapacity:     1024, // matches value used below before my changes
		BloomFunction:     MOCK_ID_HASH,
		Instrument:        instrumented.INSTRUMENT_STORE | instrumented.INSTRUMENT_ORCHESTRATOR,
	}

	sourceResponder := batch.NewSourceResponder[mock.BlockId](sourceStore, config, nil)

	// The sink is driving for pull
	sinkConnection := batch.NewGenericBatchSinkConnection[mock.BlockId](stats.GLOBAL_STATS, instrumented.INSTRUMENT_ORCHESTRATOR|instrumented.INSTRUMENT_STORE, maxBlocksPerRound, true) // Requester

	statusChannel := StatusChannel{
		make(chan *messages.StatusMessage[mock.BlockId, *mock.BlockId]),
		nil,
		bytesPerMs,
		latencyMs,
		func(*StatusChannel) Orchestrator[batch.BatchState] { return sinkConnection },
	}

	blockChannel := BlockChannel{
		make(chan *messages.BlocksMessage[mock.BlockId, *mock.BlockId]),
		nil,
		bytesPerMs,
		latencyMs,
		func(*BlockChannel) Orchestrator[batch.BatchState] {
			return sourceResponder.SourceConnection(SESSION_ID)
		},
	}

	receiverSession := sinkConnection.Session(
		NewSynchronizedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](sinkStore)),
		NewSimpleStatusAccumulator(batch.NewBloomAllocator[mock.BlockId](&config)()),
		true, // Requester
	)

	statusSender := sinkConnection.Sender(&statusChannel)

	sourceResponder.SetBatchBlockSender(&blockChannel)

	blockChannel.SetReceiverFunc(func() batch.BatchBlockReceiver[mock.BlockId] {
		return sinkConnection.Receiver(receiverSession)
	})

	statusChannel.SetReceiverFunc(func() *batch.SimpleBatchStatusReceiver[mock.BlockId] {
		return sourceResponder.Receiver(SESSION_ID)
	})

	log.Debugf("created senders and channels")

	log.Debugf("starting goroutines")

	go func() {
		log.Debugf("block listener started")
		err := blockChannel.listen()
		if err != nil {
			log.Debugf("block listener error: %s", err)
		} else {
			log.Debugf("block listener terminated")
		}
	}()

	go func() {
		log.Debugf("status listener started")
		err := statusChannel.listen()
		if err != nil {
			log.Debugf("status listener error: %s", err)
		} else {
			log.Debugf("status listener terminated")
		}
	}()

	go func() {
		log.Debugf("before enqueue")
		receiverSession.Enqueue(root)
		log.Debugf("after enqueue")

		log.Debugf("receiver session started")
		receiverSession.Run(statusSender)
	}()

	// Wait for session to start
	<-receiverSession.Started()

	go func() {
		log.Debugf("timeout started")
		time.Sleep(30 * time.Second)
		log.Debugf("timeout elapsed")

		// See what goroutine is hung
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		if err := receiverSession.Cancel(); err != nil {
			log.Errorf("error cancelling receiver session: %v", err)
		}
		if err := sourceResponder.SourceSession(SESSION_ID).Cancel(); err != nil {
			log.Errorf("error cancelling sender session: %v", err)
		}
	}()

	if err := <-receiverSession.Done(); err != nil {
		log.Debugf("receiver session failed: %v", err)
	}
	log.Debugf("receiver session terminated")

	if err := statusSender.Close(); err != nil {
		log.Errorf("error closing status sender: %v", err)
	}

	log.Debugw("waiting for all done", "sessions", len(sourceResponder.SourceSessionIds()))
	sourceResponder.AllDone()

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)
	return nil
}

func TestMockTransferToEmptyStoreSingleBatchNoDelaySend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())

	MockBatchTransferSend(senderStore, receiverStore, root, 100, 10, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatchNoDelayReceive(t *testing.T) {
	sourceStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), sourceStore, 10, 5, 0.0)
	sinkStore := mock.NewStore(mock.DefaultConfig())

	MockBatchTransferReceive(sinkStore, sourceStore, root, 100, 0, 0)
	if !sinkStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatchDelayedSend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransferSend(senderStore, receiverStore, root, 5000, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreSingleBatchDelayedReceive(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransferReceive(receiverStore, senderStore, root, 5000, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatchNoDelaySend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	MockBatchTransferSend(senderStore, receiverStore, root, 50, 10, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatchNoDelayReceive(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	MockBatchTransferReceive(receiverStore, senderStore, root, 50, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatchDelayedSend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransferSend(senderStore, receiverStore, root, 50, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockNoDelaySend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.0)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	block, err := receiverStore.RandomBlock()
	if err != nil {
		t.Errorf("Could not find random block %v", err)
	}
	receiverStore.Remove(block.Id())
	MockBatchTransferSend(senderStore, receiverStore, root, 10, 10, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockDelayedSend(t *testing.T) {
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
	MockBatchTransferSend(senderStore, receiverStore, root, 10, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingTreeNoDelaySend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	MockBatchTransferSend(senderStore, receiverStore, root, 50, 10, 0, 0)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}

func TestMockTransferSingleMissingTreeDelayedSend(t *testing.T) {
	senderStore := mock.NewStore(mock.DefaultConfig())
	mock.AddRandomForest(context.Background(), senderStore, 10)
	receiverStore := mock.NewStore(mock.DefaultConfig())
	receiverStore.AddAll(context.Background(), senderStore)
	root := mock.AddRandomTree(context.Background(), senderStore, 10, 5, 0.1)
	receiverStore.Reconfigure(blockStoreConfig)
	senderStore.Reconfigure(blockStoreConfig)
	MockBatchTransferSend(senderStore, receiverStore, root, 50, 10, GBIT_SECOND, TYPICAL_LATENCY)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
		receiverStore.Dump(root, &log.SugaredLogger, "")
	}
}
