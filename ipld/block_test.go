package ipld

import (
	"testing"

	"github.com/fission-codes/go-car-mirror/core"
	"golang.org/x/exp/slices"
)

func TestBlock(t *testing.T) {
	links := make([]Cid, 2)
	links[0] = RandId()
	links[1] = RandId()
	block, err := TryBlockFromCBOR(links)
	if err != nil {
		t.Errorf("Error creating block: %v", err)
	}
	children := block.Children()
	if !slices.Equal(links, children) {
		t.Errorf("Child array is different")
	}
}

var _ core.RawBlock[Cid] = &RawBlock{}
var _ core.Block[Cid] = &Block{}
