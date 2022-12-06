package fixtures

import (
	"testing"
)

func TestMockIdToJson(t *testing.T) {
	id := RandId()
	if buf, err := id.MarshalJSON(); err == nil {
		var id2 MockBlockId
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
		var id2 MockBlockId
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
		var id2 MockBlockId
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
