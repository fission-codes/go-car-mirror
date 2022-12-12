package ipfs

import (
	"math/rand"
	"testing"

	cid "github.com/ipfs/go-cid"

	mc "github.com/multiformats/go-multicodec"

	mh "github.com/multiformats/go-multihash"
)

func RandId() Cid {
	var hash [32]byte
	rand.Read(hash[:])
	if hashBuf, err := mh.Encode(hash[:], mh.SHA3_256); err != nil {
		panic(err)
	} else {
		ipfs_cid := cid.NewCidV1(uint64(mc.Identity), hashBuf)
		return Cid{ipfs_cid}
	}
}

func TestMockIdToJson(t *testing.T) {
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

func TestMockIdToCBOR(t *testing.T) {
	id := RandId()
	t.Logf("raw bytes: %v", id.Bytes())
	if buf, err := id.MarshalCBOR(); err == nil {
		var id2 Cid
		t.Logf("buffer %v", buf)
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

func TestMockIdToBinary(t *testing.T) {
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
