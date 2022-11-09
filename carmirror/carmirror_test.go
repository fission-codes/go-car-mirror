package carmirror

import (
	"testing"

	"github.com/fission-codes/go-car-mirror/iterator"
	gocid "github.com/ipfs/go-cid"
	// "github.com/fission-codes/go-car-mirror/bloom"
)

func TestBlock(t *testing.T) {
	// gen := blocksutil.NewBlockGenerator()
	// b := gen.Next()
	// cid := b.Cid()
	// var _

	// assert.Assert(t, !bs.Has(ib2.Cid()))
}

// --- Interface implementations ---

// BlockId
type MockBlockId struct {
	id gocid.Cid
}

func NewMockBlockId(id gocid.Cid) *MockBlockId {
	return &MockBlockId{id: id}
}

func (mid MockBlockId) String() string {
	return mid.id.String()
}

var _ BlockId = (*MockBlockId)(nil)

// Iterator
type MockBlockIdIterator = iterator.SliceIterator[MockBlockId]
type MockBlockIterator = iterator.SliceIterator[*MockBlock]

var _ iterator.Iterator[MockBlockId] = (*MockBlockIdIterator)(nil)

// BlockIdHashMap
type MockHashMap struct {
	internalMap map[string]MockBlockId
}

func NewMockHashMap() *MockHashMap {
	return &MockHashMap{
		internalMap: make(map[string]MockBlockId),
	}
}

func (m *MockHashMap) Put(id MockBlockId) error {
	m.internalMap[id.String()] = id
	return nil
}

func (m *MockHashMap) Has(id MockBlockId) (bool, error) {
	_, ok := m.internalMap[id.String()]

	return ok, nil
}

func (m *MockHashMap) Keys() (*MockBlockIdIterator, error) {
	values := make([]MockBlockId, len(m.internalMap))
	for _, v := range m.internalMap {
		values = append(values, v)
	}
	return iterator.NewSliceIterator(values), nil
}

var _ BlockIdHashMap[MockBlockId, iterator.SliceIterator[MockBlockId]] = (*MockHashMap)(nil)

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

func (b *MockBlock) Links() MockBlockIdIterator {
	return *iterator.NewSliceIterator(b.links)
}

var _ Block[MockBlockId, iterator.SliceIterator[MockBlockId]] = (*MockBlock)(nil)

// BlockStore
type MockStore struct {
	// Mapping from Cid string to MockBlock
	blocks map[string]*MockBlock
}

func NewMemoryMockBlockStore() *MockStore {
	return &MockStore{
		blocks: make(map[string]*MockBlock),
	}
}

func (bs *MockStore) Get(id MockBlockId) (*MockBlock, error) {
	return bs.blocks[id.String()], nil
}

func (bs *MockStore) Has(id MockBlockId) (bool, error) {
	_, ok := bs.blocks[id.String()]
	return ok, nil
}

func (bs *MockStore) Put(block *MockBlock) error {
	cid := block.Id()
	cidStr := cid.String()
	bs.blocks[cidStr] = block

	return nil
}

var _ BlockStore[MockBlockId, MockBlockIdIterator, MockBlockIterator, *MockBlock] = (*MockStore)(nil)

// // // MutablePointerResolver

// // // type IpfsMutablePointerResolver struct { ... }
// // // func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// // // var _ MutablePointerResolver = (...)(nil)

// // // Filter

// // type BloomFilter struct {
// // 	filter *bloom.Filter
// // }

// // // TODO: Add New* methods to mirror those in bloom.Filter

// // func (f *BloomFilter) Add(id BlockId) {
// // 	f.filter.Add(id.Bytes())
// // }

// // func (f *BloomFilter) Has(id BlockId) bool {
// // 	return f.filter.Test(id.Bytes())
// // }

// // func (f *BloomFilter) Merge(other BlockIdFilter) BloomFilter {
// // 	// TODO: Merge bloom filters together
// // 	return *f
// // }

// // var _ BlockIdFilter = (*BloomFilter)(nil)

// // // BlockSender

// // // BlockReceiver

// // // StatusAccumulator

// // // StatusSender

// // // StatusReceiver

// // // Orchestrator

// // // --- Structs and their methods not left up to implementors ---

// // // SenderSession

// // type SenderSession struct {
// // 	blockSender  BlockSender
// // 	orchestrator Orchestrator
// // 	filter       BlockIdFilter
// // 	sentCids     []BlockId // change to cid set if we don't need order
// // 	// is peer needed?  Or is this global, with peerId as a key for sentCids, like spec says
// // }

// // func (ss *SenderSession) SendBlock(BlockId)                                {}
// // func (ss *SenderSession) HandleStatus(have *BlockIdFilter, wanted BlockId) {}
// // func (ss *SenderSession) Flush()                                           {}

// // // ReceiverSession

// // type ReceiverSession struct {
// // 	statusAccumulator StatusAccumulator
// // 	statusSender      StatusSender
// // 	orchestrator      Orchestrator
// // }

// // func (rs *ReceiverSession) HandleBlock(Block)        {}
// // func (rs *ReceiverSession) AccumulateStatus(BlockId) {}
// // func (rs *ReceiverSession) Flush()                   {}

// // // SimpleStatusAccumulator

// // // --- Round based implementations ---

// // // ListSender

// // // ListReceiver

// // // BatchSendOrchestrator

// // // BatchBlockSender

// // // BatchReceiveOrchestrator

// // // BatchBlockReceiver
