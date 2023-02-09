package ipld

import (
	"math/rand"
	"testing"

	cid "github.com/ipfs/go-cid"

	mc "github.com/multiformats/go-multicodec"

	"github.com/fission-codes/go-car-mirror/core"
	mh "github.com/multiformats/go-multihash"
)

func RandId() Cid {
	var hash [32]byte
	rand.Read(hash[:])
	if hashBuf, err := mh.Encode(hash[:], mh.SHA3_256); err != nil {
		panic(err)
	} else {
		ipfsCid := cid.NewCidV1(uint64(mc.Identity), hashBuf)
		return Cid{ipfsCid}
	}
}

func TestCidToJson(t *testing.T) {
	id := RandId()
	if buf, err := id.MarshalJSON(); err == nil {
		var id2 Cid
		if err := id2.UnmarshalJSON(buf); err == nil {
			if id != id2 {
				t.Errorf("Ids no longer equal: %v != %v", id, id2)
			}
		} else {
			t.Errorf("Error marshalling Id, %v", err)
		}
	} else {
		t.Errorf("Error marshalling Id, %v", err)
	}
}

func TestCidToCBOR(t *testing.T) {
	id := RandId()
	if buf, err := id.MarshalCBOR(); err == nil {
		var id2 Cid
		if err := id2.UnmarshalCBOR(buf); err == nil {
			if id != id2 {
				t.Errorf("Ids no longer equal")
				t.Errorf(id.String())
				t.Errorf(id2.String())
			}
		} else {
			t.Errorf("Error marshalling Id, %v", err)
		}
	} else {
		t.Errorf("Error marshalling Id, %v", err)
	}
}

func TestCidToBinary(t *testing.T) {
	id := RandId()
	if buf, err := id.MarshalBinary(); err == nil {
		var id2 Cid
		if err := id2.UnmarshalBinary(buf); err == nil {
			if id != id2 {
				t.Errorf("Ids no longer equal: %v != %v", id, id2)
			}
		} else {
			t.Errorf("Error marshalling Id, %v", err)
		}
	} else {
		t.Errorf("Error marshalling Id, %v", err)
	}
}

func intoString[I core.BlockId](id I) string {
	return id.String()
}

func TestCidIsCoreId(t *testing.T) {
	id := RandId()
	if id.String() != intoString(id) {
		t.Errorf("Expected equal strings")
	}
}
