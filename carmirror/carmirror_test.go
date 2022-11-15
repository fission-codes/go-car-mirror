package carmirror

// import (
// 	"encoding/hex"
// 	"testing"

// 	"math/rand"

// 	"github.com/fission-codes/go-bloom"
// 	"github.com/fission-codes/go-car-mirror/iterator"
// )

// func TestBlock(t *testing.T) {
// 	cid := RandCid()
// 	if cid != "" {
// 		t.Errorf("cid = %v", cid)
// 	}
// }

// func RandCid() string {
// 	cid := make([]byte, 32)
// 	rand.Read(cid)
// 	return hex.EncodeToString(cid)
// }

// // --- Interface implementations ---

// // BlockId
// type MockBlockId [32]byte

// // No way to type assert comparable?
// // var _ BlockId = (*MockBlockId)(nil)

// // Iterator
// type MockBlockIdIterator = iterator.SliceIterator[MockBlockId]
// type MockBlockIterator = iterator.SliceIterator[*MockBlock]

// var _ iterator.Iterator[MockBlockId] = (*MockBlockIdIterator)(nil)

// // Block
// type MockBlock struct {
// 	id    MockBlockId
// 	links []MockBlockId
// }

// func NewMockBlock(id MockBlockId, links []MockBlockId) *MockBlock {
// 	return &MockBlock{
// 		id:    id,
// 		links: links,
// 	}
// }

// func (b *MockBlock) Id() MockBlockId {
// 	return b.id
// }

// func (b *MockBlock) Children() MockBlockIdIterator {
// 	return *iterator.NewSliceIterator(b.links)
// }

// var _ Block[MockBlockId, iterator.SliceIterator[MockBlockId]] = (*MockBlock)(nil)

// // BlockStore
// type MockStore struct {
// 	// Mapping from Cid string to MockBlock
// 	blocks map[MockBlockId]*MockBlock
// }

// func NewMockBlockStore() *MockStore {
// 	return &MockStore{
// 		blocks: make(map[MockBlockId]*MockBlock),
// 	}
// }

// func (bs *MockStore) Get(id MockBlockId) (*MockBlock, error) {
// 	return bs.blocks[id], nil
// }

// func (bs *MockStore) Has(id MockBlockId) (bool, error) {
// 	_, ok := bs.blocks[id]
// 	return ok, nil
// }

// func (bs *MockStore) All() iterator.SliceIterator[*MockBlock] {
// 	values := make([]*MockBlock, len(bs.blocks))
// 	for _, v := range bs.blocks {
// 		values = append(values, v)
// 	}
// 	return *iterator.NewSliceIterator(values)
// }

// func (bs *MockStore) Add(block *MockBlock) error {
// 	cid := block.Id()
// 	bs.blocks[cid] = block

// 	return nil
// }

// var _ BlockStore[MockBlockId, MockBlockIdIterator, *MockBlock] = (*MockStore)(nil)

// // MutablePointerResolver

// // type IpfsMutablePointerResolver struct { ... }
// // func (mpr *...) Resolve(ptr string) (id BlockId, err error) {}

// // var _ MutablePointerResolver = (...)(nil)

// // Filter

// type BloomFilter[I []byte] struct {
// 	filter *bloom.Filter
// }

// // TODO: Add New* methods to mirror those in bloom.Filter

// func (f *BloomFilter[I]) Add(id []byte) error {
// 	f.filter.Add(id)

// 	return nil
// }

// func (f *BloomFilter[I]) Has(id []byte) (bool, error) {
// 	return f.filter.Test(id), nil
// }

// func (f *BloomFilter[I]) Merge(other Filter[I]) error {
// 	// TODO: Merge bloom filters together
// 	return nil
// }

// var _ Filter[BlockId] = (*BloomFilter[BlockId])(nil)
