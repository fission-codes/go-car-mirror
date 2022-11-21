package carmirror

import (
	"fmt"
	"testing"
	"time"

	"math/rand"

	"github.com/fission-codes/go-car-mirror/iterator"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func (ch *BlockChannel) SetListener(receiver BatchBlockReceiver[MockBlockId]) {
	ch.receiver = receiver
}

func (ch *BlockChannel) run() error {
	var err error = nil
	for result := range ch.channel {
		ch.receiver.HandleList(result.status, result.blocks)
	}
	return err
}

type StatusMessage struct {
	status BatchStatus
	have   Filter[MockBlockId]
	want   []MockBlockId
}

type StatusChannel struct {
	channel        chan StatusMessage
	statusReceiver StatusReceiver[MockBlockId]
	stateReceiver  StateReceiver[BatchStatus]
	orchestrator   Orchestrator[BatchStatus]
}

func (ch *StatusChannel) SendStatus(have Filter[MockBlockId], want []MockBlockId) error {
	state, err := ch.orchestrator.GetState()
	if err != nil {
		return err
	}
	ch.channel <- StatusMessage{state, have, want}
	return nil
}

func (ch *StatusChannel) Close() error {
	close(ch.channel)
	return nil
}

func (ch *StatusChannel) SetStatusListener(receiver StatusReceiver[MockBlockId]) {
	ch.statusReceiver = receiver
}

func (ch *StatusChannel) SetStateListener(receiver StateReceiver[BatchStatus]) {
	ch.stateReceiver = receiver
}

func (ch *StatusChannel) run() error {
	var err error = nil
	for result := range ch.channel {
		ch.statusReceiver.HandleStatus(result.have, result.want)
		ch.stateReceiver.HandleState(result.status)
	}
	return err
}

type MockConnection struct {
	batchBlockChannel BlockChannel
	statusChannel     StatusChannel
	max_batch_size    uint
}

func (conn *MockConnection) GetBlockSender(orchestrator Orchestrator[BatchStatus]) BlockSender[MockBlockId] {
	return NewSimpleBatchBlockSender[MockBlockId](&conn.batchBlockChannel, orchestrator, uint32(conn.max_batch_size))
}

func (conn *MockConnection) GetStatusSender(orchestrator Orchestrator[BatchStatus]) StatusSender[MockBlockId] {
	return &conn.statusChannel
}

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

//func MockBatchTransfer(sender_store MockStore, receiver_store MockStore, root MockId, max_batch_size uint) err {
//
//	snapshotBefore := GLOBAL_REPORTING.snapshot()
//
//	connection := MockConnection::new(max_batch_size);
//
//	debug!("done setup");
//
//	let sender_session = Arc::new(SenderSession::<
//		instrument::InstrumentedStore<MockStore>,
//		MockConnection,
//		Arc<RwLock<FilterList>>,
//		instrument::InstrumentedOrchestrator<BatchSendOrchestrator>
//	>::new(instrument::instrument_store(sender_store, "sender_store"), connection.clone(), Arc::new(RwLock::new(FilterList::new(0.01, 100)))));
//
//	debug!("done sender");
//
//	let receiver_session = Arc::new(ReceiverSession::<
//		instrument::InstrumentedStore<MockStore>,
//		MockConnection,
//		SimpleStatusAccumulatorRef<MockId>,
//		BatchReceiveOrchestrator
//	>::new(instrument::instrument_store(receiver_store, "receiver_store"), connection.clone(), SimpleStatusAccumulator::new(0.01, 100)));
//
//	debug!("done receiver");
//
//	sender_session.enqueue(root).await;
//	sender_session.close().await?;
//
//	let sender_command_session = sender_session.clone();
//	let receiver_command_session = receiver_session.clone();
//
//	let sender_commands : AsyncResult<()> = Box::pin(async {
//		sender_command_session.run().await?;
//		debug!("sender session terminated");
//		Ok(())
//	});
//
//	let receiver_commands : AsyncResult<()> = Box::pin(async {
//		receiver_command_session.run().await?;
//		debug!("receiver session terminated");
//		Ok(())
//	});
//
//	try_join_all([
//		instrument::instrument_result(sender_commands, "sender_commands"),
//		instrument::instrument_result(receiver_commands, "receiver_commands"),
//		instrument::instrument_result(connection.listen_blocks(receiver_session), "receiver_session"),
//		instrument::instrument_result(connection.listen_status(sender_session), "sender_session"),
//	]).timeout(Duration::from_millis(2000))
//	.await??;
//
//	let snapshot_after = reporting.snapshot()?;
//	let diff = snapshot_before.diff(&snapshot_after);
//	info!("Dump Snapshot\n{:?}", diff);
//	Ok(())
//}
