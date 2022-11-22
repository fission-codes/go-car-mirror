package carmirror

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"math/rand"

	"github.com/fission-codes/go-car-mirror/iterator"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var ErrReceiverNotSet error = errors.New("Receiver Not Set")

func TestAntiEvergreen(t *testing.T) {
	senderStore := NewMockBlockStore()
	root := AddRandomTree(*senderStore, 5, 5, 0.0)

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

func RandId() [32]byte {
	// id := make([]byte, 32)
	var id [32]byte
	rand.Read(id[:])
	return id
}

func RandMockBlock() MockBlock {
	id := RandId()
	return *NewMockBlock(id, []MockBlockId{})
}

func RandBool(p float64) bool {
	return rand.Float64() < p
}

func AddRandomTree(store MockStore, maxChildren int, maxDepth int, pCrosslink float64) MockBlockId {
	id := RandId()
	block := NewMockBlock(id, []MockBlockId{})

	if maxDepth > 0 {
		if RandBool(pCrosslink) {
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

// BlockId
type MockBlockId [32]byte

// Iterator
type MockBlockIdIterator = iterator.SliceIterator[MockBlockId]

var _ iterator.Iterator[MockBlockId] = (*MockBlockIdIterator)(nil)

// Block
type MockBlock struct {
	id    MockBlockId
	links []MockBlockId
}

func NewMockBlock(id MockBlockId, links []MockBlockId) *MockBlock {
	return &MockBlock{
		id:    id,
		links: links,
	}
}

func (b *MockBlock) Id() MockBlockId {
	return b.id
}

func (b *MockBlock) Children() []MockBlockId {
	return b.links
}

func (b *MockBlock) AddChild(id MockBlockId) error {
	b.links = append(b.links, id)

	return nil
}

var _ Block[MockBlockId] = (*MockBlock)(nil)

// BlockStore
type MockStore struct {
	blocks map[MockBlockId]Block[MockBlockId]
}

func NewMockBlockStore() *MockStore {
	return &MockStore{
		blocks: make(map[MockBlockId]Block[MockBlockId]),
	}
}

func (bs *MockStore) Get(id MockBlockId) (Block[MockBlockId], error) {
	return bs.blocks[id], nil
}

func (bs *MockStore) Has(id MockBlockId) (bool, error) {
	_, ok := bs.blocks[id]
	return ok, nil
}

func (bs *MockStore) Remove(id MockBlockId) {
	delete(bs.blocks, id)
}

func (bs *MockStore) HasAll(root MockBlockId) bool {
	var hasAllInternal = func(root MockBlockId) error {
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

func (bs *MockStore) doHasAll(root MockBlockId) error {
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

func (bs *MockStore) All() (<-chan MockBlockId, error) {
	// values := make([]*Block[MockBlockId], len(bs.blocks))
	values := make(chan MockBlockId)
	for k, _ := range bs.blocks {
		values <- k
	}
	return values, nil
}

func (bs *MockStore) Add(block Block[MockBlockId]) error {
	id := block.Id()
	bs.blocks[id] = block

	return nil
}

func (bs *MockStore) RandomBlock() (Block[MockBlockId], error) {
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

var _ BlockStore[MockBlockId] = (*MockStore)(nil)

type BlockMessage struct {
	status BatchStatus
	blocks []Block[MockBlockId]
}

type BlockChannel struct {
	channel  chan BlockMessage
	receiver BatchBlockReceiver[MockBlockId]
}

func (ch *BlockChannel) SendList(status BatchStatus, blocks []Block[MockBlockId]) error {
	ch.channel <- BlockMessage{status, blocks}
	return nil
}

func (ch *BlockChannel) Close() error {
	close(ch.channel)
	return nil
}

func (ch *BlockChannel) SetBlockListener(receiver BatchBlockReceiver[MockBlockId]) {
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

type StatusMessage struct {
	status BatchStatus
	have   Filter[MockBlockId]
	want   []MockBlockId
}

type MockStatusReceiver struct {
	channel        <-chan StatusMessage
	statusReceiver StatusReceiver[MockBlockId, BatchStatus]
}

func (ch *MockStatusReceiver) SetStatusListener(receiver StatusReceiver[MockBlockId, BatchStatus]) {
	ch.statusReceiver = receiver
}

func (ch *MockStatusReceiver) listen() error {
	var err error = nil
	for result := range ch.channel {
		if ch.statusReceiver == nil {
			return ErrReceiverNotSet
		}
		ch.statusReceiver.HandleStatus(result.have, result.want)
		ch.statusReceiver.HandleState(result.status)
	}
	return err
}

type MockStatusSender struct {
	channel      chan<- StatusMessage
	orchestrator Orchestrator[BatchStatus]
}

func NewMockStatusSender(channel chan<- StatusMessage, orchestrator Orchestrator[BatchStatus]) *MockStatusSender {
	return &MockStatusSender{
		channel,
		orchestrator,
	}
}

func (sn *MockStatusSender) SendStatus(have Filter[MockBlockId], want []MockBlockId) error {
	state, err := sn.orchestrator.GetState()
	if err == nil {
		return err
	}
	sn.channel <- StatusMessage{state, have, want}
	return nil
}

func (sn *MockStatusSender) Close() error {
	close(sn.channel)
	return nil
}

type MockConnection struct {
	batchBlockChannel BlockChannel
	statusReceiver    MockStatusReceiver
	statusChannel     chan StatusMessage
	maxBatchSize      uint
}

func NewMockConnection(maxBatchSize uint) *MockConnection {

	statusChannel := make(chan StatusMessage)

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

func (conn *MockConnection) GetBlockSender(orchestrator Orchestrator[BatchStatus]) BlockSender[MockBlockId] {
	return NewSimpleBatchBlockSender[MockBlockId](&conn.batchBlockChannel, orchestrator, uint32(conn.maxBatchSize))
}

func (conn *MockConnection) GetStatusSender(orchestrator Orchestrator[BatchStatus]) StatusSender[MockBlockId] {
	return NewMockStatusSender(conn.statusChannel, orchestrator)
}

func (conn *MockConnection) ListenStatus(sender *SenderSession[MockBlockId, BatchStatus]) error {
	conn.statusReceiver.SetStatusListener(sender)
	return conn.statusReceiver.listen()
}

func (conn *MockConnection) ListenBlocks(receiver *ReceiverSession[MockBlockId, BatchStatus]) error {
	conn.batchBlockChannel.SetBlockListener(NewSimpleBatchBlockReceiver(receiver))
	return conn.batchBlockChannel.listen()
}

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

func MockBatchTransfer(sender_store *MockStore, receiver_store *MockStore, root MockBlockId, max_batch_size uint) error {

	snapshotBefore := GLOBAL_REPORTING.Snapshot()

	connection := NewMockConnection(max_batch_size)

	sender_session := NewSenderSession[MockBlockId, BatchStatus](
		sender_store,
		connection,
		NewRootFilter(1024, makeBloom),
		NewBatchSendOrchestrator(),
	)

	receiver_session := NewReceiverSession[MockBlockId, BatchStatus](
		receiver_store,
		connection,
		NewSimpleStatusAccumulator[MockBlockId](NewRootFilter(1024, makeBloom)),
		NewBatchReceiveOrchestrator(),
	)

	sender_session.Enqueue(root)
	sender_session.Close()

	err_chan := make(chan error)
	go func() {
		err_chan <- sender_session.Run()
		LOG.Debugf("sender session terminated")
	}()

	go func() {
		err_chan <- receiver_session.Run()
		LOG.Debugf("receiver session terminated")
	}()

	go func() {
		err_chan <- connection.ListenBlocks(receiver_session)
		LOG.Debugf("block listener terminated")
	}()

	go func() {
		err_chan <- connection.ListenStatus(sender_session)
		LOG.Debugf("status listener terminated")
	}()

	for i := 0; i < 4; i++ {
		err := <-err_chan
		if err != nil {
			return err
		}
	}

	snapshotAfter := GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(LOG)
	return nil
}
