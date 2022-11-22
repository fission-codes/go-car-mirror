package carmirror

import (
	"errors"
	"testing"

	"github.com/fission-codes/go-bloom"
	"github.com/zeebo/xxh3"
)

func IdHash(id MockBlockId, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}

func populateFilter(filter Filter[MockBlockId], count int) ([]MockBlockId, error) {
	pf := make([]MockBlockId, 0, count)
	var err error
	for i := 0; i < count && err == nil; i++ {
		id := RandId()
		err = filter.Add(id)
		pf = append(pf, id)
	}
	return pf, err
}

func checkPresent(filter Filter[MockBlockId], ids []MockBlockId) error {
	for _, id := range ids {
		if filter.DoesNotContain(id) {
			return errors.New("failed to retrieve item")
		}
	}
	return nil
}

func ModelFilterTest(filter Filter[MockBlockId], t *testing.T) {

	ids, err := populateFilter(filter, 40)
	if err != nil {
		t.Errorf("failed to insert item")
	} else {
		if checkPresent(filter, ids) != nil {
			t.Errorf("failed to retrieve item")
		}
	}

	fpCount := 0
	for i := 0; i < 40; i++ {
		id := RandId()
		if !filter.DoesNotContain(id) {
			fpCount++
		}
	}

	if fpCount > 4 {
		t.Errorf("%v false positives, which is far over expectation", fpCount)
	}
}

func TestBloomFilter(t *testing.T) {
	ModelFilterTest(NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](64, IdHash), t)
}

func TestRootFilter(t *testing.T) {
	ModelFilterTest(NewRootFilter(64, makeBloom), t)
}

func TestBloomOverflow(t *testing.T) {

	bloom := NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](32, IdHash)

	errCount := 0
	for i := 0; i < 64; i++ {
		id := RandId()
		err := bloom.Add(id)
		if err != nil {
			if err == ErrBloomOverflow {
				errCount++
			} else {
				t.Errorf("Unexpected error %v", err)
			}
		}
	}

	if errCount < 16 {
		t.Errorf("Did not see expected overflow errors")
	}
}

func ModelTestAddAll(filterA Filter[MockBlockId], filterB Filter[MockBlockId], t *testing.T) {

	idsA, errA := populateFilter(filterA, 32)
	idsB, errB := populateFilter(filterB, 32)

	if errA != nil {
		t.Errorf("Unexpected error %v", errA)
	}
	if errB != nil {
		t.Errorf("Unexpected error %v", errB)
	}

	err := filterA.AddAll(filterB)

	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}

	err = checkPresent(filterA, idsA)

	if err != nil {
		t.Errorf("Failed to retrieve expected items from combined filter")
	}

	err = checkPresent(filterA, idsB)

	if err != nil {
		t.Errorf("Failed to retrieve expected items from combined filter")
	}
}

func TestBloomAddAll(t *testing.T) {
	ModelTestAddAll(
		NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](128, IdHash),
		NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](128, IdHash),
		t,
	)
}

func TestRootAddAll(t *testing.T) {
	ModelTestAddAll(
		NewRootFilter(128, makeBloom),
		NewRootFilter(128, makeBloom),
		t,
	)
}

func TestBloomAddAllOverflow(t *testing.T) {

	bloomA := NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](128, IdHash)
	bloomB := NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](128, IdHash)

	_, errA := populateFilter(bloomA, 100)
	_, errB := populateFilter(bloomB, 100)

	if errA != nil {
		t.Errorf("Unexpected error %v", errA)
	}
	if errB != nil {
		t.Errorf("Unexpected error %v", errB)
	}

	err := bloomA.AddAll(bloomB)

	if err != ErrBloomOverflow {
		t.Errorf("Unexpected error %v", err)
	}
}

func TestRootAddAllNoOverflow(t *testing.T) {
	ModelTestAddAll(
		NewRootFilter(52, makeBloom),
		NewRootFilter(52, makeBloom),
		t,
	)
}

func makeBloom(capacity uint) Filter[MockBlockId] {
	return NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](capacity, IdHash)
}
