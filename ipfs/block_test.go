package ipfs

import (
	"testing"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	cbor "github.com/fxamacker/cbor/v2"
	ipld_cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	"golang.org/x/exp/slices"
)

func TestBlock(t *testing.T) {
	links := make([]Cid, 2)
	links[0] = RandId()
	links[1] = RandId()
	cborNode, err := cbor.Marshal(links)
	if err != nil {
		t.Errorf("Error converting to cbor %v", err)
	}
	ipldNode, err := ipld_cbor.Decode(cborNode, mh.SHA2_256, 32)
	if err != nil {
		t.Errorf("Error converting to node %v", err)

	}
	block := WrapBlock(ipldNode)
	children := block.GetChildren()
	if !slices.Equal(links, children) {
		t.Errorf("Child array is different")
	}
}

var _ core.RawBlock[Cid] = &RawBlock{}
