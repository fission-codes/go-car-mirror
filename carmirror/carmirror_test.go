package carmirror

import (
	"encoding/hex"
	"testing"

	"math/rand"

	"github.com/fission-codes/go-bloom"
	"github.com/fission-codes/go-car-mirror/iterator"
)

func TestBlock(t *testing.T) {
	cid := RandCid()
	if cid == "" {
		t.Errorf("cid = %v", cid)
	}
}

func RandCid() string {
	cid := make([]byte, 32)
	rand.Read(cid)
	return hex.EncodeToString(cid)
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

func (b *MockBlock) Id() *MockBlockId {
	return &b.id
}

func (b *MockBlock) Children() iterator.Iterator[MockBlockId] {
	return *iterator.NewSliceIterator(b.links)
}

var _ Block[MockBlockId] = (*MockBlock)(nil)

// BlockStore
type MockStore struct {
	// Mapping from Cid string to MockBlock
	blocks map[MockBlockId]Block[MockBlockId]
}

func NewMockBlockStore() *MockStore {
	return &MockStore{
		blocks: make(map[MockBlockId]Block[MockBlockId]),
	}
}

func (bs *MockStore) Get(id *MockBlockId) (Block[MockBlockId], error) {
	return bs.blocks[*id], nil
}

func (bs *MockStore) Has(id *MockBlockId) (bool, error) {
	_, ok := bs.blocks[*id]
	return ok, nil
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
	bid := block.Id()
	bs.blocks[*bid] = block

	return nil
}

var _ BlockStore[MockBlockId] = (*MockStore)(nil)

// MutablePointerResolver

// type IpfsMutablePointerResolver struct { ... }
// func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// var _ MutablePointerResolver = (...)(nil)

// Filter

type BloomFilter[I MockBlockId] struct {
	filter *bloom.Filter
}

// TODO: Add New* methods to mirror those in bloom.Filter

func (f *BloomFilter[I]) Add(id MockBlockId) error {
	f.filter.Add(id[:])

	return nil
}

func (f *BloomFilter[I]) DoesNotContain(id MockBlockId) bool {
	return !f.filter.Test(id[:])
}

func (f *BloomFilter[I]) Merge(other Filter[I]) (Filter[MockBlockId], error) {
	// TODO: Merge bloom filters together.  Update those methods to not mutate.
	return nil, nil
}

var _ Filter[MockBlockId] = (*BloomFilter[MockBlockId])(nil)
