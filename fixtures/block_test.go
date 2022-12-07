package fixtures

import (
	"testing"

	"golang.org/x/exp/slices"
)

func TestMockIdToJson(t *testing.T) {
	id := RandId()
	if buf, err := id.MarshalJSON(); err == nil {
		var id2 BlockId
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
	if buf, err := id.MarshalCBOR(); err == nil {
		var id2 BlockId
		if err := id2.UnmarshalCBOR(buf); err == nil {
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

func TestMockIdToBinary(t *testing.T) {
	id := RandId()
	if buf, err := id.MarshalBinary(); err == nil {
		var id2 BlockId
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

func TestMockBlockStableBytes(t *testing.T) {
	block := NewBlock(RandId(), 1024)
	if !slices.Equal(block.Bytes(), block.Bytes()) {
		t.Errorf("Unstable byte array, size 1024")
	}
	block = NewBlock(RandId(), 10240)
	if !slices.Equal(block.Bytes(), block.Bytes()) {
		t.Errorf("Unstable byte array, size 10240")
	}
	block = NewBlock(RandId(), 102400)
	if !slices.Equal(block.Bytes(), block.Bytes()) {
		t.Errorf("Unstable byte array, size 102400")
	}
}
