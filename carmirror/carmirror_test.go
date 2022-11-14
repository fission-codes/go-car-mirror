package carmirror

import (
	"testing"

	"github.com/fission-codes/go-bloom"
	"github.com/fission-codes/go-car-mirror/iterator"
	gocid "github.com/ipfs/go-cid"
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

func (mid MockBlockId) Bytes() []byte {
	return mid.id.Bytes()
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

func (m *MockHashMap) Add(id MockBlockId) error {
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

func (b *MockBlock) Children() MockBlockIdIterator {
	return *iterator.NewSliceIterator(b.links)
}

var _ Block[MockBlockId, iterator.SliceIterator[MockBlockId]] = (*MockBlock)(nil)

// BlockStore
type MockStore struct {
	// Mapping from Cid string to MockBlock
	blocks map[string]*MockBlock
}

func NewMockBlockStore() *MockStore {
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

func (bs *MockStore) All() iterator.SliceIterator[*MockBlock] {
	values := make([]*MockBlock, len(bs.blocks))
	for _, v := range bs.blocks {
		values = append(values, v)
	}
	return *iterator.NewSliceIterator(values)
}

func (bs *MockStore) Add(block *MockBlock) error {
	cid := block.Id()
	cidStr := cid.String()
	bs.blocks[cidStr] = block

	return nil
}

var _ BlockStore[MockBlockId, MockBlockIdIterator, MockBlockIterator, *MockBlock] = (*MockStore)(nil)

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

type BloomFilter[I BlockId] struct {
	filter *bloom.Filter
}

// TODO: Add New* methods to mirror those in bloom.Filter

func (f *BloomFilter[I]) Add(id I) error {
	f.filter.Add(id.Bytes())

	return nil
}

func (f *BloomFilter[I]) Has(id I) (bool, error) {
	return f.filter.Test(id.Bytes()), nil
}

func (f *BloomFilter[I]) Merge(other BlockIdFilter[I]) error {
	// TODO: Merge bloom filters together
	return nil
}

var _ BlockIdFilter[BlockId] = (*BloomFilter[BlockId])(nil)
