package ipfs

import (
	block "github.com/ipfs/go-block-format"
	node "github.com/ipfs/go-ipld-format"
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
