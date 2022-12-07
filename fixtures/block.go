package fixtures

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	cmerrors "github.com/fission-codes/go-car-mirror/errors"

	"github.com/fxamacker/cbor/v2"
)

func RandId() MockBlockId {
	var id MockBlockId
	rand.Read(id[:])
	return id
}

func RandMockBlock() *MockBlock {
	id := RandId()
	return NewMockBlock(id, int64(rand.Intn(10240)))
}

// BlockId
type MockBlockId [32]byte

func (id MockBlockId) String() string {
	return base64.URLEncoding.EncodeToString(id[:])
}

func (id MockBlockId) MarshalBinary() ([]byte, error) {
	return id[:], nil
}

func (id *MockBlockId) UnmarshalBinary(bytes []byte) error {
	if len(bytes) < 32 {
		return errors.New("bad size for id")
	}
	copy(id[:], bytes)
	return nil
}

func (id MockBlockId) MarshalCBOR() ([]byte, error) {
	if bytes, error := id.MarshalBinary(); error == nil {
		return cbor.Marshal(bytes)
	} else {
		return nil, error
	}
}

func (id *MockBlockId) UnmarshalCBOR(bytes []byte) error {
	var rawbytes []byte
	if error := cbor.Unmarshal(bytes, &rawbytes); error == nil {
		return id.UnmarshalBinary(rawbytes)
	} else {
		return error
	}
}

type MockIdJsonFormat struct {
	Id []byte `json:"id"`
}

func (id MockBlockId) MarshalJSON() ([]byte, error) {
	return json.Marshal(MockIdJsonFormat{Id: id[:]})
}

func (id *MockBlockId) UnmarshalJSON(bytes []byte) error {
	var data MockIdJsonFormat
	if err := json.Unmarshal(bytes, &data); err == nil {
		copy(id[:], data.Id)
		return nil
	} else {
		return err
	}
}

func (id *MockBlockId) Read(reader io.ByteReader) (int, error) {
	var err error
	for i := 0; i < 32 && err == nil; i++ {
		id[i], err = reader.ReadByte()
	}
	return 32, err
}

// Block
type MockBlock struct {
	id    MockBlockId
	links []MockBlockId
	size  int64
}

func NewMockBlock(id MockBlockId, size int64) *MockBlock {
	return &MockBlock{
		id:    id,
		links: make([]MockBlockId, 0, 10),
		size:  size,
	}
}

func (b *MockBlock) Id() MockBlockId {
	return b.id
}

func (b *MockBlock) Bytes() []byte {
	var buf bytes.Buffer
	source := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(b.id[0:8]) >> 1)))
	count, err := io.CopyN(&buf, source, b.size)
	if err != nil {
		panic(err)
	}
	if count != b.size {
		panic("couldn't write enough bytes")
	}
	return buf.Bytes()
}

func (b *MockBlock) Size() int64 {
	return b.size
}

func (b *MockBlock) Children() []MockBlockId {
	return b.links
}

func (b *MockBlock) AddChild(id MockBlockId) error {
	b.links = append(b.links, id)

	return nil
}

func AddRandomTree(store *MockStore, maxChildren int, maxDepth int, pCrosslink float64) MockBlockId {
	id := RandId()
	block := NewMockBlock(id, rand.Int63n(10240))

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

func AddRandomForest(store *MockStore, rootCount int) []MockBlockId {
	roots := make([]MockBlockId, rootCount)
	for i := 0; i < rootCount; i++ {
		roots[i] = AddRandomTree(store, 10, 5, 0.05)
	}
	return roots
}

// BlockStore
type MockStore struct {
	blocks map[MockBlockId]core.Block[MockBlockId]
}

func NewMockStore() *MockStore {
	return &MockStore{
		blocks: make(map[MockBlockId]core.Block[MockBlockId]),
	}
}

func (bs *MockStore) Get(id MockBlockId) (core.Block[MockBlockId], error) {
	block, ok := bs.blocks[id]
	if !ok || block == nil {
		return nil, cmerrors.ErrBlockNotFound
	}

	return block, nil
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
	values := make(chan MockBlockId, len(bs.blocks))
	for _, v := range bs.blocks {
		values <- v.Id()
	}
	close(values)
	return values, nil
}

func (bs *MockStore) Add(rawBlock core.RawBlock[MockBlockId]) (core.Block[MockBlockId], error) {
	block, ok := rawBlock.(*MockBlock)
	if ok {
		id := block.Id()
		bs.blocks[id] = block
		return block, nil
	} else {
		return nil, errors.New("MockStore can only store MockBlocks")
	}
}

func (bs *MockStore) AddAll(store core.BlockStore[MockBlockId]) error {
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

func (bs *MockStore) RandomBlock() (core.Block[MockBlockId], error) {
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

func RandBool(p float64) bool {
	return rand.Float64() < p
}
