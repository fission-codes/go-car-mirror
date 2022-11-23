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

func populateFilter(filter Filter[MockBlockId], count int) (Filter[MockBlockId], []MockBlockId) {
	pf := make([]MockBlockId, 0, count)
	for i := 0; i < count; i++ {
		id := RandId()
		filter = filter.Add(id)
		pf = append(pf, id)
	}
	return filter, pf
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

	filter, ids := populateFilter(filter, 40)
	if checkPresent(filter, ids) != nil {
		t.Errorf("failed to retrieve item")
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
	ModelFilterTest(makeBloom(64), t)
}

func TestRootFilter(t *testing.T) {
	ModelFilterTest(NewRootFilter[MockBlockId](makeBloom(64)), t)
}

func TestBloomOverflow(t *testing.T) {

	var bt Filter[MockBlockId] = makeBloom(32)

	for i := 0; i < 52; i++ {
		id := RandId()
		bt = bt.Add(id)
	}

	cf, ok := bt.(*CompoundFilter[MockBlockId])

	if ok {
		bfa, ok := cf.a.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
		if ok {
			if bfa.count != 32 {
				t.Errorf("Expected side a to be fully populated, but got %v items", bfa.count)
			}
		} else {
			t.Errorf("Expected side a to be a bloom")
		}
		bfb, ok := cf.b.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
		if ok {
			if bfb.count != 20 {
				t.Errorf("Expected side b to have 20 items, but got %v items", bfb.count)
			}
		} else {
			t.Errorf("Expected side b to be a bloom")
		}
	} else {
		t.Errorf("Expected a compound filter after bloom overflow")
	}
}

func ModelTestAddAll(filterA Filter[MockBlockId], filterB Filter[MockBlockId], t *testing.T) {

	filterA, idsA := populateFilter(filterA, 32)
	filterB, idsB := populateFilter(filterB, 32)

	filterA = filterA.AddAll(filterB)

	err := checkPresent(filterA, idsA)

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
		makeBloom(128),
		makeBloom(128),
		t,
	)
}

func TestRootAddAll(t *testing.T) {
	ModelTestAddAll(
		NewRootFilter(makeBloom(128)),
		NewRootFilter(makeBloom(128)),
		t,
	)
}

func TestBloomAddAllOverflow(t *testing.T) {

	var bloomA Filter[MockBlockId] = makeBloom(128)
	var bloomB Filter[MockBlockId] = makeBloom(128)

	bloomA, _ = populateFilter(bloomA, 100)
	bloomB, _ = populateFilter(bloomB, 100)

	bloomA = bloomA.AddAll(bloomB)

	cf, ok := bloomA.(*CompoundFilter[MockBlockId])

	if ok {
		bfa, ok := cf.a.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
		if ok {
			if bfa.count != 100 {
				t.Errorf("Expected side a to have 100 items, but got %v items", bfa.count)
			}
		} else {
			t.Errorf("Expected side a to be a bloom")
		}
		bfb, ok := cf.b.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
		if ok {
			if bfb.count != 100 {
				t.Errorf("Expected side b to have 100 items, but got %v items", bfb.count)
			}
		} else {
			t.Errorf("Expected side b to be a bloom")
		}
	} else {
		t.Errorf("Expected a compound filter after bloom overflow")
	}

}

func TestBloomAddAllWithCommon(t *testing.T) {

	var bloomA Filter[MockBlockId] = makeBloom(128)
	var bloomB Filter[MockBlockId] = makeBloom(128)
	var bloomC Filter[MockBlockId] = makeBloom(128)

	bloomA, _ = populateFilter(bloomA, 40)
	bloomB, _ = populateFilter(bloomB, 40)
	bloomC, _ = populateFilter(bloomC, 40)

	bloomA = bloomA.AddAll(bloomC)
	bloomB = bloomB.AddAll(bloomC)

	_, ok := bloomA.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
	if !ok {
		t.Errorf("Expected bloomA to still be a BloomFilter")
	}

	_, ok = bloomB.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
	if !ok {
		t.Errorf("Expected bloomB to still be a BloomFilter")
	}

	bloomA = bloomA.AddAll(bloomB)
	_, ok = bloomA.(*BloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]])
	if !ok {
		t.Errorf("Expected bloomA to still be a BloomFilter")
	}

	estimate := bloomA.GetCount()
	if estimate < 100 || estimate > 140 {
		t.Errorf("Expected bloomA count to be between 100 and 140; got %v", estimate)
	}
}

func TestRootAddAllNoOverflow(t *testing.T) {
	ModelTestAddAll(
		NewRootFilter(makeBloom(52)),
		NewRootFilter(makeBloom(52)),
		t,
	)
}

func makeBloom(capacity uint) Filter[MockBlockId] {
	return NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](capacity, IdHash)
}
