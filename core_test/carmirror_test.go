package core_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"math/rand"

	. "github.com/fission-codes/go-car-mirror/carmirror"
	cmerrors "github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log *zap.SugaredLogger

func init() {
	rand.Seed(time.Now().UnixNano())
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	log = logger.Sugar()
}

var ErrReceiverNotSet error = errors.New("receiver not set")

func TestAntiEvergreen(t *testing.T) {
	senderStore := NewMockStore()
	root := AddRandomTree(senderStore, 5, 5, 0.0)

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

func RandBool(p float64) bool {
	return rand.Float64() < p
}

func IdHash(id fixtures.MockBlockId, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}

func makeBloom(capacity uint) filter.Filter[fixtures.MockBlockId] {
	return filter.NewBloomFilter(capacity, IdHash)
}

func AddRandomTree(store *MockStore, maxChildren int, maxDepth int, pCrosslink float64) fixtures.MockBlockId {
	id := fixtures.RandId()
	block := fixtures.NewMockBlock(id, rand.Int63n(10240))

	if maxDepth > 0 {
		if RandBool(pCrosslink) && len(store.blocks) > 0 {
			existingBlock, err := store.RandomBlock()
			if err != nil {
				panic(err)
			}
			block.AddChild(existingBlock.Id())
		} else {
			// gen rand num children
			children := rand.Intn(maxChildren)
			for child := 0; child < children; child++ {
				childMinDepth := maxDepth / 2
				childMaxDepth := rand.Intn(maxDepth-childMinDepth) + childMinDepth
				block.AddChild(AddRandomTree(store, maxChildren, childMaxDepth, pCrosslink))
			}
		}
	}

	store.Add(block)

	return id
}

func AddRandomForest(store *MockStore, rootCount int) []fixtures.MockBlockId {
	roots := make([]fixtures.MockBlockId, rootCount)
	for i := 0; i < rootCount; i++ {
		roots[i] = AddRandomTree(store, 10, 5, 0.05)
	}
	return roots
}

// BlockStore
type MockStore struct {
	blocks map[fixtures.MockBlockId]Block[fixtures.MockBlockId]
}

func NewMockStore() *MockStore {
	return &MockStore{
		blocks: make(map[fixtures.MockBlockId]Block[fixtures.MockBlockId]),
	}
}

func (bs *MockStore) Get(id fixtures.MockBlockId) (Block[fixtures.MockBlockId], error) {
	block, ok := bs.blocks[id]
	if !ok || block == nil {
		return nil, cmerrors.ErrBlockNotFound
	}

	return block, nil
}

func (bs *MockStore) Has(id fixtures.MockBlockId) (bool, error) {
	_, ok := bs.blocks[id]
	return ok, nil
}

func (bs *MockStore) Remove(id fixtures.MockBlockId) {
	delete(bs.blocks, id)
}

func (bs *MockStore) HasAll(root fixtures.MockBlockId) bool {
	var hasAllInternal = func(root fixtures.MockBlockId) error {
		if b, ok := bs.blocks[root]; ok {
			for _, child := range b.Children() {
				if err := bs.doHasAll(child); err != nil {
					return err
				}
			}
			return nil
		} else {
			return fmt.Errorf("Missing block %x", root)
		}
	}
	return hasAllInternal(root) == nil
}

func (bs *MockStore) doHasAll(root fixtures.MockBlockId) error {
	if b, ok := bs.blocks[root]; ok {
		for _, child := range b.Children() {
			if err := bs.doHasAll(child); err != nil {
				return err
			}
		}
		return nil
	} else {
		return fmt.Errorf("Missing block %x", root)
	}
}

func (bs *MockStore) All() (<-chan fixtures.MockBlockId, error) {
	values := make(chan fixtures.MockBlockId, len(bs.blocks))
	for _, v := range bs.blocks {
		values <- v.Id()
	}
	close(values)
	return values, nil
}

func (bs *MockStore) Add(rawBlock RawBlock[fixtures.MockBlockId]) (Block[fixtures.MockBlockId], error) {
	block, ok := rawBlock.(*fixtures.MockBlock)
	if ok {
		id := block.Id()
		bs.blocks[id] = block
		return block, nil
	} else {
		return nil, errors.New("MockStore can only store MockBlocks")
	}
}

func (bs *MockStore) AddAll(store BlockStore[fixtures.MockBlockId]) error {
	if blocks, err := store.All(); err == nil {
		for id := range blocks {
			if block, err := store.Get(id); err == nil {
				_, err = bs.Add(block)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		return nil
	} else {
		return err
	}
}

func (bs *MockStore) RandomBlock() (Block[fixtures.MockBlockId], error) {
	if len(bs.blocks) == 0 {
		return nil, fmt.Errorf("No blocks in store")
	}

	i := rand.Intn(len(bs.blocks))
	for _, v := range bs.blocks {
		if i == 0 {
			return v, nil
		}
		i--
	}

	return nil, fmt.Errorf("This should never happen")
}

type BlockMessage struct {
	status BatchStatus
	blocks []RawBlock[fixtures.MockBlockId]
}

type BlockChannel struct {
	channel  chan BlockMessage
	receiver BatchBlockReceiver[fixtures.MockBlockId]
}

func (ch *BlockChannel) SendList(status BatchStatus, blocks []RawBlock[fixtures.MockBlockId]) error {
	ch.channel <- BlockMessage{status, blocks}
	return nil
}

func (ch *BlockChannel) Close() error {
	close(ch.channel)
	return nil
}

func (ch *BlockChannel) SetBlockListener(receiver BatchBlockReceiver[fixtures.MockBlockId]) {
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

type MockStatusMessage messages.StatusMessage[fixtures.MockBlockId, *fixtures.MockBlockId, filter.Filter[fixtures.MockBlockId], BatchStatus]

type MockStatusReceiver struct {
	channel        <-chan MockStatusMessage
	statusReceiver StatusReceiver[fixtures.MockBlockId, BatchStatus]
}

func (ch *MockStatusReceiver) SetStatusListener(receiver StatusReceiver[fixtures.MockBlockId, BatchStatus]) {
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

func (sn *MockStatusSender) SendStatus(have filter.Filter[fixtures.MockBlockId], want []fixtures.MockBlockId) error {
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

func (conn *MockConnection) OpenBlockSender(orchestrator Orchestrator[BatchStatus]) BlockSender[fixtures.MockBlockId] {
	return NewInstrumentedBlockSender[fixtures.MockBlockId](
		NewSimpleBatchBlockSender[fixtures.MockBlockId](&conn.batchBlockChannel, orchestrator, uint32(conn.maxBatchSize)),
		GLOBAL_STATS.WithContext("MockBlockSender"),
	)
}

func (conn *MockConnection) OpenStatusSender(orchestrator Orchestrator[BatchStatus]) StatusSender[fixtures.MockBlockId] {
	return NewInstrumentedStatusSender[fixtures.MockBlockId](
		NewMockStatusSender(conn.statusChannel, orchestrator),
		GLOBAL_STATS.WithContext("MockStatusSender"),
	)
}

func (conn *MockConnection) ListenStatus(sender StatusReceiver[fixtures.MockBlockId, BatchStatus]) error {
	conn.statusReceiver.SetStatusListener(sender)
	return conn.statusReceiver.listen()
}

func (conn *MockConnection) ListenBlocks(receiver BlockReceiver[fixtures.MockBlockId, BatchStatus]) error {
	conn.batchBlockChannel.SetBlockListener(NewSimpleBatchBlockReceiver(receiver))
	return conn.batchBlockChannel.listen()
}

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

func MockBatchTransfer(sender_store *MockStore, receiver_store *MockStore, root fixtures.MockBlockId, max_batch_size uint) error {

	snapshotBefore := GLOBAL_REPORTING.Snapshot()
	log = zap.S()

	connection := NewMockConnection(max_batch_size)

	sender_session := NewSenderSession[fixtures.MockBlockId, BatchStatus](
		NewInstrumentedBlockStore[fixtures.MockBlockId](sender_store, GLOBAL_STATS.WithContext("SenderStore")),
		connection,
		filter.NewSynchronizedFilter(makeBloom(1024)),
		NewInstrumentedOrchestrator[BatchStatus](NewBatchSendOrchestrator(), GLOBAL_STATS.WithContext("BatchSendOrchestrator")),
	)

	log.Debugf("created sender_session")

	receiver_session := NewReceiverSession[fixtures.MockBlockId, BatchStatus](
		NewInstrumentedBlockStore[fixtures.MockBlockId](NewSynchronizedBlockStore[fixtures.MockBlockId](receiver_store), GLOBAL_STATS.WithContext("ReceiverStore")),
		connection,
		NewSimpleStatusAccumulator[fixtures.MockBlockId](filter.NewSynchronizedFilter(makeBloom(1024))),
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
		err_chan <- connection.ListenStatus(NewInstrumentedStatusReceiver[fixtures.MockBlockId, BatchStatus](sender_session, GLOBAL_STATS.WithContext("StatusListener")))
		log.Debugf("status listener terminated")
	}()

	go func() {
		log.Debugf("timeout started")
		time.Sleep(5 * time.Second)
		log.Debugf("timeout elapsed")
		if !sender_session.IsClosed() {
			sender_session.Cancel()
		}
		if !receiver_session.IsClosed() {
			receiver_session.Cancel()
		}
	}()

	for i := 0; i < 4; i++ {
		err := <-err_chan
		log.Debugf("goroutine terminated with %v", err)
		if err != nil {
			return err
		}
	}

	snapshotAfter := GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(log)
	return nil
}

func InitLog() {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.DebugLevel)
	zap.ReplaceGlobals(zap.Must(config.Build()))
	InitDefault()
}

func TestMockTransferToEmptyStoreSingleBatch(t *testing.T) {
	InitLog()
	InitDefault()
	senderStore := NewMockStore()
	root := AddRandomTree(senderStore, 5, 5, 0.0)
	receiverStore := NewMockStore()
	MockBatchTransfer(senderStore, receiverStore, root, 5000)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferToEmptyStoreMultiBatch(t *testing.T) {
	InitLog()
	InitDefault()
	senderStore := NewMockStore()
	root := AddRandomTree(senderStore, 5, 5, 0.0)
	receiverStore := NewMockStore()
	MockBatchTransfer(senderStore, receiverStore, root, 10)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}

func TestMockTransferSingleMissingBlockBatch(t *testing.T) {
	InitLog()
	InitDefault()
	senderStore := NewMockStore()
	root := AddRandomTree(senderStore, 5, 5, 0.0)
	receiverStore := NewMockStore()
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
	InitLog()
	InitDefault()
	senderStore := NewMockStore()
	AddRandomForest(senderStore, 10)
	receiverStore := NewMockStore()
	receiverStore.AddAll(senderStore)
	root := AddRandomTree(senderStore, 10, 5, 0.1)
	MockBatchTransfer(senderStore, receiverStore, root, 10)
	if !receiverStore.HasAll(root) {
		t.Errorf("Expected receiver store to have all nodes")
	}
}
