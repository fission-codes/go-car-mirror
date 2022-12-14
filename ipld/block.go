package ipld

import (
	cbor "github.com/fxamacker/cbor/v2"
	block "github.com/ipfs/go-block-format"
	ipld_cbor "github.com/ipfs/go-ipld-cbor"
	node "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// RawBlock wraps the block.Block interface, allowing it to interface with the carmirror code base.
type RawBlock struct {
	block.Block
}

// Id returns the Cid of the block.
func (blk *RawBlock) Id() Cid {
	return WrapCid(blk.Cid())
}

// Size returns the size of the block.
func (blk *RawBlock) Size() int64 {
	return int64(len(blk.RawData()))
}

// Unwrap returns the underlying block.
func (blk *RawBlock) Unwrap() block.Block {
	return blk.Block
}

// WrapRawBlock wraps a block.Block in a RawBlock.
func WrapRawBlock(block block.Block) *RawBlock {
	return &RawBlock{block}
}

// Block wraps the node.Node interface, adding some extra methods to interface with the carmirror code base.
type Block struct {
	node.Node
}

// Unwrap returns the underlying node.
func (blk *Block) Unwrap() node.Node {
	return blk.Node
}

// WrapBlock wraps a node.Node in a Block.
func WrapBlock(node node.Node) *Block {
	return &Block{node}
}

// Id returns the Cid of the block.
func (blk *Block) Id() Cid {
	return WrapCid(blk.Cid())
}

// Size returns the size of the block.
func (blk *Block) Size() int64 {
	uint64, err := blk.Node.Size()
	if err != nil {
		panic(err)
	}
	return int64(uint64)
}

// GetChildren returns the Cids of the children of the block.
func (blk *Block) Children() []Cid {
	links := blk.Links()
	result := make([]Cid, len(links))
	for i, node := range blk.Links() {
		result[i] = WrapCid(node.Cid)
	}
	return result
}

func TryBlockFromCBOR(cborData any) (*Block, error) {
	cborNode, err := cbor.Marshal(cborData)
	if err != nil {
		return nil, err
	}
	ipldNode, err := ipld_cbor.Decode(cborNode, mh.SHA2_256, 32)
	if err != nil {
		return nil, err

	}
	return WrapBlock(ipldNode), nil
}
