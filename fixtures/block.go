package fixtures

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/fission-codes/go-car-mirror/core"
	cmerrors "github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/util"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/fxamacker/cbor/v2"
)

// var log = golog.Logger("go-car-mirror")

const BLOCK_ID_SIZE = 32

// Defines a simple block 256-bit BlockId
type BlockId [BLOCK_ID_SIZE]byte

// Generate a random block Id
func RandId() BlockId {
	var id BlockId
	rand.Read(id[:])
	return id
}

// Generate a random block up to 10k bytes in length
// Does not allocate memory for the byte array
func RandMockBlock() *Block {
	id := RandId()
	return NewBlock(id, int64(rand.Intn(10240)))
}

// Returns a URL-encoded base 64 string
func (id BlockId) String() string {
	return base64.URLEncoding.EncodeToString(id[:])
}

// Gets the data for this block as a byte array
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

func (id *BlockId) Read(reader core.ByteAndBlockReader) (int, error) {
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

func (b *Block) RawData() []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(b.links))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.BigEndian, b.links); err != nil {
		panic(err)
	}
	remainingBytes := b.Size() - 2 - int64(len(b.links))*BLOCK_ID_SIZE
	source := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(b.id[0:8]) >> 1)))
	count, err := io.CopyN(&buf, source, remainingBytes)
	if err != nil {
		panic(err)
	}
	if count != remainingBytes {
		panic("couldn't write enough bytes")
	}
	return buf.Bytes()
}

// private function used by BlockStore
func (b *Block) setBytes(data []byte) error {
	var reader = bytes.NewBuffer(data)
	var lenLinks uint16
	if err := binary.Read(reader, binary.BigEndian, &lenLinks); err != nil {
		return err
	}
	b.links = make([]BlockId, lenLinks)
	if err := binary.Read(reader, binary.BigEndian, &b.links); err != nil {
		return err
	}
	// now check remaining bytes
	source := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(b.id[0:8]) >> 1)))
	remainder := reader.Bytes()
	random := make([]byte, len(remainder))
	source.Read(random)
	if !slices.Equal(remainder, random) {
		return errors.New("tried to set bytes incompatible with Id")
	} else {
		return nil
	}
}

func (b *Block) Size() int64 {
	return util.Max(b.size, int64(len(b.links))*BLOCK_ID_SIZE+2)
}

func (b *Block) Children() []BlockId {
	return b.links
}

func (b *Block) AddChild(id BlockId) error {
	b.links = append(b.links, id)
	return nil
}

func AddRandomTree(ctx context.Context, store *Store, maxChildren int, maxDepth int, pCrosslink float64) BlockId {
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
			children := 2 + rand.Intn(maxChildren/2) + rand.Intn(maxChildren/2) // makes number of children cluster around average
			for child := 0; child < children; child++ {
				childMaxDepth := util.Max(0, maxDepth-rand.Intn(2)-1)
				block.AddChild(AddRandomTree(ctx, store, maxChildren, childMaxDepth, pCrosslink))
			}
		}
	}

	store.Add(ctx, block)

	return id
}

func AddRandomForest(ctx context.Context, store *Store, rootCount int) []BlockId {
	roots := make([]BlockId, rootCount)
	for i := 0; i < rootCount; i++ {
		roots[i] = AddRandomTree(ctx, store, 10, 5, 0.05)
	}
	return roots
}

type Config struct {
	WriteStorageLatency  time.Duration
	WriteStorageBandwith time.Duration // time to write one byte
	ReadStorageLatency   time.Duration
	ReadStorageBandwith  time.Duration // time to write one byte
}

func DefaultConfig() Config {
	return Config{
		0, 0, 0, 0,
	}
}

// BlockStore
type Store struct {
	blocks map[BlockId]core.Block[BlockId]
	config *Config
	mutex  sync.Mutex
}

func NewStore(config Config) *Store {
	return &Store{
		blocks: make(map[BlockId]core.Block[BlockId]),
		config: &config,
		mutex:  sync.Mutex{},
	}
}

func (bs *Store) Reconfigure(config Config) {
	bs.config = &config
}

func (bs *Store) Get(_ context.Context, id BlockId) (core.Block[BlockId], error) {
	time.Sleep(bs.config.ReadStorageLatency)
	bs.mutex.Lock()
	block, ok := bs.blocks[id]
	bs.mutex.Unlock()
	if !ok || block == nil {
		return nil, cmerrors.ErrBlockNotFound
	}
	time.Sleep(bs.config.ReadStorageBandwith * time.Duration(block.Size()))
	return block, nil
}

func (bs *Store) Dump(id BlockId, log *zap.SugaredLogger, spacer string) (core.Block[BlockId], error) {
	bs.mutex.Lock()
	block, ok := bs.blocks[id]
	bs.mutex.Unlock()
	if ok {
		log.Info(fmt.Sprintf("%s%s", spacer, id.String()))
		childSpacer := spacer + "  "
		for _, child := range block.Children() {
			bs.Dump(child, log, childSpacer)
		}
	} else {
		log.Info(fmt.Sprintf("%s<not present>", spacer))
	}

	return block, nil
}

func (bs *Store) Has(_ context.Context, id BlockId) (bool, error) {
	time.Sleep(bs.config.ReadStorageLatency)
	bs.mutex.Lock()
	_, ok := bs.blocks[id]
	bs.mutex.Unlock()
	return ok, nil
}

func (bs *Store) Remove(id BlockId) {
	time.Sleep(bs.config.WriteStorageLatency)
	bs.mutex.Lock()
	delete(bs.blocks, id)
	bs.mutex.Unlock()
}

func (bs *Store) HasAll(root BlockId) bool {
	var hasAllInternal = func(root BlockId) error {
		bs.mutex.Lock()
		b, ok := bs.blocks[root]
		bs.mutex.Unlock()
		if ok {
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
	bs.mutex.Lock()
	b, ok := bs.blocks[root]
	bs.mutex.Unlock()
	if ok {
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

func (bs *Store) All(_ context.Context) (<-chan BlockId, error) {
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
	values := make(chan BlockId, len(bs.blocks))
	for _, v := range bs.blocks {
		time.Sleep(bs.config.ReadStorageLatency)
		values <- v.Id()
	}
	close(values)
	return values, nil
}

func (bs *Store) Add(_ context.Context, rawBlock core.RawBlock[BlockId]) (core.Block[BlockId], error) {
	time.Sleep(bs.config.WriteStorageLatency + time.Duration(rawBlock.Size())*bs.config.WriteStorageBandwith)
	block, ok := rawBlock.(*Block)
	if !ok {
		block = NewBlock(rawBlock.Id(), rawBlock.Size())
		block.setBytes(rawBlock.RawData())
	}
	id := block.Id()
	bs.mutex.Lock()
	bs.blocks[id] = block
	bs.mutex.Unlock()
	return block, nil
}

func (bs *Store) AddMany(_ context.Context, rawBlocks []core.RawBlock[BlockId]) ([]core.Block[BlockId], error) {
	var blocks []core.Block[BlockId]
	for _, rawBlock := range rawBlocks {
		time.Sleep(bs.config.WriteStorageLatency + time.Duration(rawBlock.Size())*bs.config.WriteStorageBandwith)
		block, ok := rawBlock.(*Block)
		if !ok {
			block = NewBlock(rawBlock.Id(), rawBlock.Size())
			block.setBytes(rawBlock.RawData())
		}
		id := block.Id()
		bs.mutex.Lock()
		bs.blocks[id] = block
		blocks = append(blocks, block)
		bs.mutex.Unlock()
	}
	return blocks, nil
}

func (bs *Store) AddAll(ctx context.Context, store core.BlockStore[BlockId]) error {
	if blocks, err := store.All(ctx); err == nil {
		for id := range blocks {
			if block, err := store.Get(ctx, id); err == nil {
				_, err = bs.Add(ctx, block)
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
	bs.mutex.Lock()
	defer bs.mutex.Unlock()
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

func XX3HashBlockId(id BlockId, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}
