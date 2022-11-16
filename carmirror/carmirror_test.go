package carmirror

import (
	"fmt"
	"testing"
	"time"

	"math/rand"

	"github.com/fission-codes/go-bloom"
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
	// Mapping from Cid string to MockBlock
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
	err := bs.doHasAll(root)
	return err == nil
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
