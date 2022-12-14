package ipld

import (
	cbor "github.com/fxamacker/cbor/v2"
	block "github.com/ipfs/go-block-format"
	ipld_cbor "github.com/ipfs/go-ipld-cbor"
	node "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

// Wrap the generic block interface
type RawBlock struct {
	block.Block
}

func (blk *RawBlock) Id() Cid {
	return WrapCid(blk.Cid())
}

func (blk *RawBlock) Size() int64 {
	return int64(len(blk.RawData()))
}

func (blk *RawBlock) Unwrap() block.Block {
	return blk.Block
}

func WrapRawBlock(block block.Block) *RawBlock {
	return &RawBlock{block}
}

type Block struct {
	node.Node
}

func (blk *Block) Unwrap() node.Node {
	return blk.Node
}

func WrapBlock(node node.Node) *Block {
	return &Block{node}
}

func (blk *Block) Id() Cid {
	return WrapCid(blk.Cid())
}

func (blk *Block) Size() int64 {
	uint64, err := blk.Node.Size()
	if err != nil {
		panic(err)
	}
	return int64(uint64)
}

func (blk *Block) GetChildren() []Cid {
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
