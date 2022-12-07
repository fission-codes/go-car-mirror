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

func RandId() BlockId {
	var id BlockId
	rand.Read(id[:])
	return id
}

func RandMockBlock() *Block {
	id := RandId()
	return NewBlock(id, int64(rand.Intn(10240)))
}

// BlockId
type BlockId [32]byte

func (id BlockId) String() string {
	return base64.URLEncoding.EncodeToString(id[:])
}

func (id BlockId) MarshalBinary() ([]byte, error) {
	return id[:], nil
}

func (id *BlockId) UnmarshalBinary(bytes []byte) error {
	if len(bytes) < 32 {
		return errors.New("bad size for id")
	}
	copy(id[:], bytes)
	return nil
}

func (id BlockId) MarshalCBOR() ([]byte, error) {
	if bytes, error := id.MarshalBinary(); error == nil {
		return cbor.Marshal(bytes)
	} else {
		return nil, error
	}
}

func (id *BlockId) UnmarshalCBOR(bytes []byte) error {
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

func (id BlockId) MarshalJSON() ([]byte, error) {
	return json.Marshal(MockIdJsonFormat{Id: id[:]})
}

func (id *BlockId) UnmarshalJSON(bytes []byte) error {
	var data MockIdJsonFormat
	if err := json.Unmarshal(bytes, &data); err == nil {
		copy(id[:], data.Id)
		return nil
	} else {
		return err
	}
}

func (id *BlockId) Read(reader io.ByteReader) (int, error) {
	var err error
	for i := 0; i < 32 && err == nil; i++ {
		id[i], err = reader.ReadByte()
	}
	return 32, err
}

// Block
type Block struct {
	id    BlockId
	links []BlockId
	size  int64
}

func NewBlock(id BlockId, size int64) *Block {
	return &Block{
		id:    id,
		links: make([]BlockId, 0, 10),
		size:  size,
	}
}

func (b *Block) Id() BlockId {
	return b.id
}

func (b *Block) Bytes() []byte {
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

func (b *Block) Size() int64 {
	return b.size
}

func (b *Block) Children() []BlockId {
	return b.links
}

func (b *Block) AddChild(id BlockId) error {
	b.links = append(b.links, id)

	return nil
}

func AddRandomTree(store *Store, maxChildren int, maxDepth int, pCrosslink float64) BlockId {
	id := RandId()
	block := NewBlock(id, rand.Int63n(10240))

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

func AddRandomForest(store *Store, rootCount int) []BlockId {
	roots := make([]BlockId, rootCount)
	for i := 0; i < rootCount; i++ {
		roots[i] = AddRandomTree(store, 10, 5, 0.05)
	}
	return roots
}

// BlockStore
type Store struct {
	blocks map[BlockId]core.Block[BlockId]
}

func NewStore() *Store {
	return &Store{
		blocks: make(map[BlockId]core.Block[BlockId]),
	}
}

func (bs *Store) Get(id BlockId) (core.Block[BlockId], error) {
	block, ok := bs.blocks[id]
	if !ok || block == nil {
		return nil, cmerrors.ErrBlockNotFound
	}

	return block, nil
}

func (bs *Store) Has(id BlockId) (bool, error) {
	_, ok := bs.blocks[id]
	return ok, nil
}

func (bs *Store) Remove(id BlockId) {
	delete(bs.blocks, id)
}

func (bs *Store) HasAll(root BlockId) bool {
	var hasAllInternal = func(root BlockId) error {
		if b, ok := bs.blocks[root]; ok {
			for _, child := range b.Children() {
				if err := bs.doHasAll(child); err != nil {
					return err
				}
			}
			return nil
		} else {
			return fmt.Errorf("missing block %x", root)
		}
	}
	return hasAllInternal(root) == nil
}

func (bs *Store) doHasAll(root BlockId) error {
	if b, ok := bs.blocks[root]; ok {
		for _, child := range b.Children() {
			if err := bs.doHasAll(child); err != nil {
				return err
			}
		}
		return nil
	} else {
		return fmt.Errorf("missing block %x", root)
	}
}

func (bs *Store) All() (<-chan BlockId, error) {
	values := make(chan BlockId, len(bs.blocks))
	for _, v := range bs.blocks {
		values <- v.Id()
	}
	close(values)
	return values, nil
}

func (bs *Store) Add(rawBlock core.RawBlock[BlockId]) (core.Block[BlockId], error) {
	block, ok := rawBlock.(*Block)
	if ok {
		id := block.Id()
		bs.blocks[id] = block
		return block, nil
	} else {
		return nil, errors.New("MockStore can only store MockBlocks")
	}
}

func (bs *Store) AddAll(store core.BlockStore[BlockId]) error {
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

func (bs *Store) RandomBlock() (core.Block[BlockId], error) {
	if len(bs.blocks) == 0 {
		return nil, fmt.Errorf("no blocks in store")
	}

	i := rand.Intn(len(bs.blocks))
	for _, v := range bs.blocks {
		if i == 0 {
			return v, nil
		}
		i--
	}

	return nil, fmt.Errorf("this should never happen")
}

func RandBool(p float64) bool {
	return rand.Float64() < p
}
