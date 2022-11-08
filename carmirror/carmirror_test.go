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
var _ BlockId = (*gocid.Cid)(nil)

// Iterator
var _ iterator.Iterator[gocid.Cid] = (*iterator.SliceIterator[gocid.Cid])(nil)

// BlockIdHashMap
type MockHashMap[I BlockId, IT iterator.Iterator[I]] struct {
	internalMap map[string]I
}

func NewMockHashMap[I BlockId, IT iterator.Iterator[I]]() *MockHashMap[I, IT] {
	return &MockHashMap[I, IT]{
		internalMap: make(map[string]I),
	}
}

func (m *MockHashMap[I, IT]) Put(id I) error {
	m.internalMap[id.String()] = id
	return nil
}

func (m *MockHashMap[I, IT]) Has(id I) (bool, error) {
	_, ok := m.internalMap[id.String()]

	return ok, nil
}

func (m *MockHashMap[I, IT]) Keys() (iterator.SliceIterator[I], error) {
	values := make([]I, len(m.internalMap))
	for _, v := range m.internalMap {
		values = append(values, v)
	}
	return iterator.NewSliceIterator(values), nil
}

var _ BlockIdHashMap[gocid.Cid, iterator.SliceIterator[gocid.Cid]] = (*MockHashMap[gocid.Cid, iterator.Iterator[gocid.Cid]])(nil)

// Block
type IpldBlock[I gocid.Cid] struct {
	id       I
	rawBytes []byte
	links    []I
}

func NewIpldBlock[I gocid.Cid](id I, rawBytes []byte, links []I) *IpldBlock[I] {
	return &IpldBlock[I]{
		id:       id,
		rawBytes: rawBytes,
		links:    links,
	}
}

func (b *IpldBlock[I]) Id() I {
	return b.id
}

func (b *IpldBlock[I]) RawBytes() []byte {
	return b.rawBytes
}

func (b *IpldBlock[I]) Links() iterator.Iterator[I] {
	return iterator.NewSliceIterator(b.links)
}

var _ Block[gocid.Cid] = (*IpldBlock[gocid.Cid])(nil)

// // // BlockStore
// // type MemoryIpldBlockStore struct {
// // 	// Mapping from Cid string to IpldBlock
// // 	blocks map[string]*Block
// // }

// // func NewMemoryIpldBlockStore() *MemoryIpldBlockStore {
// // 	return &MemoryIpldBlockStore{
// // 		blocks: make(map[string]*Block),
// // 	}
// // }

// // func (bs *MemoryIpldBlockStore) Get(cid BlockId) Block {
// // 	return *bs.blocks[cid.String()]
// // }

// // func (bs *MemoryIpldBlockStore) Has(cid BlockId) bool {
// // 	_, ok := bs.blocks[cid.String()]
// // 	return ok
// // }

// // func (bs *MemoryIpldBlockStore) Put(block Block) {
// // 	cid := block.Id().String()
// // 	bs.blocks[cid] = &block
// // }

// // var _ BlockStore = (*MemoryIpldBlockStore)(nil)

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
