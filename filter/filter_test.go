package filter

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zeebo/xxh3"
)

type TestId [32]byte

func RandId() [32]byte {
	// id := make([]byte, 32)
	var id [32]byte
	rand.Read(id[:])
	return id
}

func IdHash(id TestId, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}

func populateFilter(filter Filter[TestId], count int) (Filter[TestId], []TestId) {
	pf := make([]TestId, 0, count)
	for i := 0; i < count; i++ {
		id := RandId()
		filter = filter.Add(id)
		pf = append(pf, id)
	}
	return filter, pf
}

func checkPresent(filter Filter[TestId], ids []TestId) error {
	for _, id := range ids {
		if filter.DoesNotContain(id) {
			return errors.New("failed to retrieve item")
		}
	}
	return nil
}

func constituentFilters(cf *CompoundFilter[TestId]) []Filter[TestId] {
	var result []Filter[TestId]
	cfa, ok := cf.SideA.(*CompoundFilter[TestId])
	if ok {
		result = append(result, constituentFilters(cfa)...)
	} else {
		result = append(result, cf.SideA)
	}
	cfb, ok := cf.SideB.(*CompoundFilter[TestId])
	if ok {
		result = append(result, constituentFilters(cfb)...)
	} else {
		result = append(result, cf.SideB)
	}
	return result
}

func ModelFilterTest(filter Filter[TestId], t *testing.T) {

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

func TestSynchronizedFilter(t *testing.T) {
	ModelFilterTest(NewSynchronizedFilter(makeBloom(64)), t)
}

func TestBloomOverflow(t *testing.T) {

	var bt Filter[TestId] = makeBloom(32)

	for i := 0; i < 52; i++ {
		id := RandId()
		bt = bt.Add(id)
	}

	cf, ok := bt.(*CompoundFilter[TestId])

	if ok {
		bfa, ok := cf.SideA.(*BloomFilter[TestId])
		if ok {
			if bfa.count != 32 {
				t.Errorf("Expected side a to be fully populated, but got %v items", bfa.count)
			}
		} else {
			t.Errorf("Expected side a to be a bloom")
		}
		bfb, ok := cf.SideB.(*BloomFilter[TestId])
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

func ModelTestAddAll(filterA Filter[TestId], filterB Filter[TestId], t *testing.T) {

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
		NewSynchronizedFilter(makeBloom(128)),
		NewSynchronizedFilter(makeBloom(128)),
		t,
	)
}

func TestBloomAddAllOverflow(t *testing.T) {

	var bloomA Filter[TestId] = makeBloom(128)
	var bloomB Filter[TestId] = makeBloom(128)

	bloomA, _ = populateFilter(bloomA, 100)
	bloomB, _ = populateFilter(bloomB, 100)

	bloomA = bloomA.AddAll(bloomB)

	cf, ok := bloomA.(*CompoundFilter[TestId])

	if ok {
		bfa, ok := cf.SideA.(*BloomFilter[TestId])
		if ok {
			if bfa.count != 100 {
				t.Errorf("Expected side a to have 100 items, but got %v items", bfa.count)
			}
		} else {
			t.Errorf("Expected side a to be a bloom")
		}
		bfb, ok := cf.SideB.(*BloomFilter[TestId])
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

	var bloomA Filter[TestId] = makeBloom(128)
	var bloomB Filter[TestId] = makeBloom(128)
	var bloomC Filter[TestId] = makeBloom(128)

	bloomA, _ = populateFilter(bloomA, 40)
	bloomB, _ = populateFilter(bloomB, 40)
	bloomC, _ = populateFilter(bloomC, 40)

	bloomA = bloomA.AddAll(bloomC)
	bloomB = bloomB.AddAll(bloomC)

	_, ok := bloomA.(*BloomFilter[TestId])
	if !ok {
		t.Errorf("Expected bloomA to still be a BloomFilter")
	}

	_, ok = bloomB.(*BloomFilter[TestId])
	if !ok {
		t.Errorf("Expected bloomB to still be a BloomFilter")
	}

	bloomA = bloomA.AddAll(bloomB)
	_, ok = bloomA.(*BloomFilter[TestId])
	if !ok {
		t.Errorf("Expected bloomA to still be a BloomFilter")
	}

	estimate := bloomA.Count()
	if estimate < 100 || estimate > 140 {
		t.Errorf("Expected bloomA count to be between 100 and 140; got %v", estimate)
	}
}

func TestRootAddAllNoOverflow(t *testing.T) {
	ModelTestAddAll(
		NewSynchronizedFilter(makeBloom(52)),
		NewSynchronizedFilter(makeBloom(52)),
		t,
	)
}

func TestAddAllCompound(t *testing.T) {
	filterA := makeBloom(32)
	filterB := makeBloom(32)

	filterA, idsA := populateFilter(filterA, 40)
	filterB, idsB := populateFilter(filterB, 40)

	_, ok := filterA.(*CompoundFilter[TestId])

	if !ok {
		t.Errorf("Expected filterA to be a combined filter")
	}

	_, ok = filterB.(*CompoundFilter[TestId])

	if !ok {
		t.Errorf("Expected filterB to be a combined filter")
	}

	filterA = filterA.AddAll(filterB)

	err := checkPresent(filterA, idsA)

	if err != nil {
		t.Errorf("Failed to retrieve expected items from combined filter")
	}

	err = checkPresent(filterA, idsB)

	if err != nil {
		t.Errorf("Failed to retrieve expected items from combined filter")
	}

	// Filter A should at this point have three separate constituent filters - two saturated filters, and one unsaturated
	cfA, ok := filterA.(*CompoundFilter[TestId])

	if ok {
		filters := constituentFilters(cfA)
		if len(filters) != 3 {
			t.Errorf("Resulting compound filter has size %v, expected 3", len(filters))
		}
	} else {
		t.Errorf("expected final filter to be a compound filter")
	}
}

func TestBloomSerializationJson(t *testing.T) {
	bloom1 := makeBloom(64)
	bytes, err := json.Marshal(bloom1)
	if err != nil {
		t.Errorf("Expected bloom to marshal to Json, got error %v instead", err)
	} else {
		bloom2 := BloomFilter[TestId]{filter: nil, capacity: 0, count: 0, hashFunction: 0}
		err = json.Unmarshal(bytes, &bloom2)
		if err != nil {
			t.Errorf("Expected json %s to unmarshal to bloom, got error %v instead", bytes, err)
		}
		if !bloom1.Equal(&bloom2) {
			t.Errorf("Expected blooms to be equal after serialization/deserialization")
		}
	}
}

const TEST_HASHER = 19710403 // No siginificance to this number

func makeBloom(capacity uint) Filter[TestId] {
	// Normally this would occur in package init code, but this is just a test
	RegisterHash(TEST_HASHER, IdHash)
	if new, err := TryNewBloomFilter[TestId](capacity, TEST_HASHER); err == nil {
		return new
	} else {
		panic("failed to instantiate test bloom")
	}

}
