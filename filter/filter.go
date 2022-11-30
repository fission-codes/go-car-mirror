package filter

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"sync"

	"github.com/fission-codes/go-bloom"
)

// Filter is anything similar to a bloom filter that can efficiently (and without perfect accuracy) keep track of a list of `BlockId`s.
type Filter[K comparable] interface {
	// DoesNotContain returns true if id is not present in the filter.
	DoesNotContain(id K) bool
	// Get the total capacity of this filter; may return -1 to indicate unconstrained
	Capacity() int
	// Returns an estimate of the number of entries in the filter
	Count() int
	// Returns a copy or reference as appropriate
	Copy() Filter[K]
	// Compare with some other filter
	Equal(Filter[K]) bool
	// Mutate operations follow the go convention normally mutating but returning a new filter when necessary
	Add(id K) Filter[K]
	Clear() Filter[K]
	// TryAddAll fails if the 'other' filter cannot be added
	TryAddAll(other Filter[K]) error
	// Add All will create a new filter if necessary to accomodate the merge
	AddAll(other Filter[K]) Filter[K]
}

type Side uint8

const (
	SIDE_A Side = 0
	SIDE_B Side = 1
)

var ErrIncompatibleFilter = errors.New("incompatible filter type")
var ErrBloomOverflow = errors.New("bloom filter overflowing")
var ErrCantMarshalBloomWithoutHashId = errors.New("Bloom must be created with a hash Id in order to marshal it")

// A compound filter implements the union of two disparate filter types
type CompoundFilter[K comparable] struct {
	SideA Filter[K]
	SideB Filter[K]
}

func (cf *CompoundFilter[K]) DoesNotContain(id K) bool {
	return cf.SideA.DoesNotContain(id) && cf.SideB.DoesNotContain(id)
}

func (cf *CompoundFilter[K]) GetSparser() Side {
	// -1 indicates unlimited capacity, therefore automatically the sparser of two sides
	if cf.SideA.Capacity() < 0 || cf.SideB.Capacity() < 0 {
		if cf.SideA.Capacity() >= 0 {
			return SIDE_B
		}
	} else {
		// Otherwise find the side with the most unused capacity
		if cf.SideB.Capacity()-cf.SideB.Count() > cf.SideA.Capacity()-cf.SideA.Count() {
			return SIDE_B
		}
	}
	return SIDE_A
}

func (cf *CompoundFilter[K]) TryAddAll(other Filter[K]) error {
	// Try to add to the sparser of the two sides. If this fails,
	// try adding to the other side. If this fails, try adding on
	// on the less sparse side
	var err error
	if cf.GetSparser() == SIDE_A {
		err = cf.SideA.TryAddAll(other)
	} else {
		err = cf.SideB.TryAddAll(other)
	}

	if err != nil {
		if cf.GetSparser() == SIDE_A {
			err = cf.SideB.TryAddAll(other)
		} else {
			err = cf.SideA.TryAddAll(other)
		}
	}
	return err
}

func (cf *CompoundFilter[K]) AddAll(other Filter[K]) Filter[K] {
	ocf, ok := other.(*CompoundFilter[K])
	if ok {
		return cf.AddAll(ocf.SideA).AddAll(ocf.SideB)
	} else {
		err := cf.TryAddAll(other)
		if err == nil {
			return cf
		} else {
			return &CompoundFilter[K]{cf, other}
		}
	}
}

func (cf *CompoundFilter[K]) Add(id K) Filter[K] {
	// Try to add to the sparser side. If this fails,
	// add to the other side.
	if cf.GetSparser() == SIDE_A {
		cf.SideA = cf.SideA.Add(id)
	} else {
		cf.SideB = cf.SideB.Add(id)
	}
	return cf
}

func (cf *CompoundFilter[K]) Clear() Filter[K] {
	if cf.SideA.Capacity() < 0 || cf.SideB.Capacity() > 0 && cf.SideA.Capacity() > cf.SideB.Capacity() {
		return cf.SideA.Clear()
	} else {
		return cf.SideB.Clear()
	}
}

func (cf *CompoundFilter[K]) Copy() Filter[K] {
	return &CompoundFilter[K]{cf.SideA.Copy(), cf.SideB.Copy()}
}

func (cf *CompoundFilter[K]) Capacity() int {
	if cf.SideA.Capacity() < 0 || cf.SideB.Capacity() < 0 {
		return -1
	} else {
		return cf.SideA.Capacity() + cf.SideB.Capacity()
	}
}

func (cf *CompoundFilter[K]) Count() int {
	return cf.SideA.Count() + cf.SideB.Count()
}

func (cf *CompoundFilter[K]) Equal(other Filter[K]) bool {
	ocf, ok := other.(*CompoundFilter[K])
	return ok && cf.SideA.Equal(ocf.SideA) && cf.SideB.Equal(ocf.SideB)
}

// A bloom filter is a classic, fixed-capacity bloom filter which also stores an approximate count of the number of entries
type BloomFilter[K comparable] struct {
	filter       *bloom.Filter[K]
	capacity     int
	count        int
	hashFunction uint64
}

// Create a bloom filter with the given estimated capacity (The false positive rate is determined by bloom.EstimateFPP)
func NewBloomFilter[K comparable](capacity uint, function bloom.HashFunction[K]) *BloomFilter[K] {
	if capacity > math.MaxInt {
		capacity = math.MaxInt
	}
	bitCount, hashCount := bloom.EstimateParameters(uint64(capacity), bloom.EstimateFPP(uint64(capacity)))
	filter, _ := bloom.NewFilter[K](bitCount, hashCount, function)

	return &BloomFilter[K]{
		filter:       filter,
		capacity:     int(capacity),
		count:        0,
		hashFunction: 0,
	}
}

// Create a bloom filter from a prepopulated byte array and related low-level information including the hash function
func NewBloomFilterFromBytes[K comparable](bytes []byte, bitCount uint64, hashCount uint64, function bloom.HashFunction[K]) *BloomFilter[K] {
	filter := bloom.NewFilterFromBloomBytes[K](bitCount, hashCount, bytes, function)

	return &BloomFilter[K]{
		filter:       filter,
		capacity:     int(filter.EstimateCapacity()),
		count:        int(filter.EstimateEntries()),
		hashFunction: 0,
	}
}

// create a bloom filter of the given capacity using the registered hash function Id, failing if no such hash function can be found
func TryNewBloomFilter[K comparable](capacity uint, function uint64) (*BloomFilter[K], error) {
	if hashFunction, ok := RegistryLookup[K](function); ok {
		filter := NewBloomFilter[K](capacity, hashFunction)
		filter.hashFunction = function
		return filter, nil
	} else {
		return nil, ErrIncompatibleFilter
	}
}

func TryNewBloomFilterFromBytes[K comparable](bytes []byte, bitCount uint64, hashCount uint64, function uint64) (*BloomFilter[K], error) {
	if hashFunction, ok := RegistryLookup[K](function); ok {
		filter := NewBloomFilterFromBytes[K](bytes, bitCount, hashCount, hashFunction)
		filter.hashFunction = function
		return filter, nil
	} else {
		return nil, ErrIncompatibleFilter
	}
}

// When adding a new item would saturate the filter, create a compound filter composed of the existing filter plus a new bloom of double the capacity
func (f *BloomFilter[K]) Add(id K) Filter[K] {
	if f.count < f.capacity {
		if !f.filter.Test(id) {
			f.count = f.count + 1
			f.filter.Add(id)
		}
		return f
	} else {
		var extension Filter[K] = NewBloomFilter[K](uint(f.capacity)*2, f.filter.HashFunction())
		extension = extension.Add(id)
		return &CompoundFilter[K]{
			SideA: f,
			SideB: extension,
		}
	}
}

func (f *BloomFilter[K]) DoesNotContain(id K) bool {
	return !f.filter.Test(id)
}

func (f *BloomFilter[K]) TryAddAll(other Filter[K]) error {
	obf, ok := other.(*BloomFilter[K])
	if ok {
		if f.capacity < f.count+obf.count {
			working_bloom := f.filter.Copy()
			err := working_bloom.Union(obf.filter)
			if err != nil {
				return err
			}
			new_count := int(working_bloom.EstimateEntries())
			if new_count >= f.capacity {
				return ErrBloomOverflow
			}
			f.filter = working_bloom
			f.count = new_count
		} else {
			err := f.filter.Union(obf.filter)
			if err != nil {
				return err
			}
			f.count = int(f.filter.EstimateEntries())
		}
		return nil
	} else {
		return ErrIncompatibleFilter
	}
}

func (f *BloomFilter[K]) AddAll(other Filter[K]) Filter[K] {
	err := f.TryAddAll(other)
	if err == nil {
		return f
	} else {
		return &CompoundFilter[K]{
			SideA: f,
			SideB: other.Copy(),
		}
	}
}

func (f *BloomFilter[K]) Capacity() int {
	return f.capacity
}

func (f *BloomFilter[K]) Count() int {
	return f.count
}

func (f *BloomFilter[K]) Clear() Filter[K] {
	return NewBloomFilter[K](uint(f.capacity), f.filter.HashFunction())
}

func (f *BloomFilter[K]) Copy() Filter[K] {
	return &BloomFilter[K]{
		filter:   f.filter.Copy(),
		capacity: f.capacity,
		count:    f.count,
	}
}

func (bf *BloomFilter[K]) Equal(other Filter[K]) bool {
	obf, ok := other.(*BloomFilter[K])
	return ok &&
		bytes.Equal(bf.filter.Bytes(), obf.filter.Bytes()) &&
		bf.filter.HashCount() == obf.filter.HashCount() &&
		bf.filter.BitCount() == obf.filter.BitCount() &&
		bf.hashFunction == obf.hashFunction
}

type BloomWireFormat struct {
	Bytes        []byte
	HashFunction uint64
	HashCount    uint64
	BitCount     uint64
}

func (bf *BloomFilter[K]) UnmarshalJSON(bytes []byte) error {
	wireFormat := BloomWireFormat{}
	err := json.Unmarshal(bytes, &wireFormat)
	if err != nil {
		return err
	}

	if hashFunction, ok := RegistryLookup[K](wireFormat.HashFunction); ok {
		bf.filter = bloom.NewFilterFromBloomBytes(wireFormat.BitCount, wireFormat.HashCount, wireFormat.Bytes, hashFunction)
		bf.capacity = int(bf.filter.EstimateCapacity())
		bf.count = int(bf.filter.EstimateEntries())
		bf.hashFunction = wireFormat.HashFunction
		return nil
	} else {
		return ErrIncompatibleFilter
	}
}

func (bf *BloomFilter[K]) MarshalJSON() ([]byte, error) {
	if bf.hashFunction != 0 {
		return json.Marshal(&BloomWireFormat{bf.filter.Bytes(), bf.hashFunction, bf.filter.HashCount(), bf.filter.BitCount()})
	} else {
		return nil, ErrCantMarshalBloomWithoutHashId
	}
}

// A thread-safe Filter which wraps a regular Filter with a mutex.
type SynchronizedFilter[K comparable] struct {
	filter Filter[K]
	lock   sync.RWMutex
}

func NewSynchronizedFilter[K comparable](filter Filter[K]) *SynchronizedFilter[K] {
	return &SynchronizedFilter[K]{filter: filter, lock: sync.RWMutex{}}
}

func (rf *SynchronizedFilter[K]) DoesNotContain(id K) bool {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.DoesNotContain(id)
}

func (rf *SynchronizedFilter[K]) TryAddAll(other Filter[K]) error {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	return rf.filter.TryAddAll(other)
}

func (rf *SynchronizedFilter[K]) AddAll(other Filter[K]) Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.AddAll(other)
	return rf
}

func (rf *SynchronizedFilter[K]) Add(id K) Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.Add(id)
	return rf
}

func (rf *SynchronizedFilter[K]) Clear() Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.Clear()
	return rf
}

func (rf *SynchronizedFilter[K]) Copy() Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	return &SynchronizedFilter[K]{filter: rf.filter.Copy(), lock: sync.RWMutex{}}
}

func (rf *SynchronizedFilter[K]) Capacity() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.Capacity()
}

func (rf *SynchronizedFilter[K]) Count() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.Count()
}

func (sf *SynchronizedFilter[K]) Equal(other Filter[K]) bool {
	osf, ok := other.(*SynchronizedFilter[K])
	if !ok {
		return false
	}
	sf.lock.RLock()
	osf.lock.RLock()
	defer sf.lock.RUnlock()
	defer osf.lock.RUnlock()
	return sf.filter.Equal(osf.filter)
}

// PerfectFilter is a Filter implementation which uses and underlying map - mainly for testing
type PerfectFilter[K comparable] struct {
	filter map[K]bool
}

func NewPerfectFilter[K comparable]() *PerfectFilter[K] {
	return &PerfectFilter[K]{make(map[K]bool)}
}

func (pf *PerfectFilter[K]) DoesNotContain(id K) bool {
	return !pf.filter[id]
}

func (pf *PerfectFilter[K]) TryAddAll(other Filter[K]) error {
	opf, ok := other.(*PerfectFilter[K])
	if ok {
		for k := range opf.filter {
			pf.filter[k] = true
		}
		return nil
	} else {
		return ErrIncompatibleFilter
	}
}

func (pf *PerfectFilter[K]) AddAll(other Filter[K]) Filter[K] {
	err := pf.TryAddAll(other)
	if err == nil {
		return pf
	} else {
		return &CompoundFilter[K]{SideA: pf, SideB: other.Copy()}
	}
}

func (pf *PerfectFilter[K]) Add(id K) Filter[K] {
	pf.filter[id] = true
	return pf
}

func (pf *PerfectFilter[K]) Clear() Filter[K] {
	pf.filter = make(map[K]bool)
	return pf
}

func (pf *PerfectFilter[K]) Copy() Filter[K] {
	return NewPerfectFilter[K]().AddAll(pf)
}

func (pf *PerfectFilter[K]) Capacity() int {
	return -1 // capacity of perfect filter is unconstrained
}

func (pf *PerfectFilter[K]) Count() int {
	return len(pf.filter)
}

func (pf *PerfectFilter[K]) Equal(other Filter[K]) bool {
	opf, ok := other.(*PerfectFilter[K])
	if !ok {
		return false
	}
	if len(pf.filter) != len(opf.filter) {
		return false
	}
	for key, _ := range pf.filter {
		if _, ok := opf.filter[key]; !ok {
			return false
		}
	}
	return true
}

// EmptyFilter represents a filter which contains no entries. An allocator is provided which is used to create a new filter if necessary.
type EmptyFilter[K comparable] struct {
	allocator func() Filter[K]
}

func (ef *EmptyFilter[K]) DoesNotContain(id K) bool {
	return true
}

func (ef *EmptyFilter[K]) TryAddAll(other Filter[K]) error {
	return ErrIncompatibleFilter
}

func (ef *EmptyFilter[K]) AddAll(other Filter[K]) Filter[K] {
	return other.Copy()
}

func (ef *EmptyFilter[K]) Add(id K) Filter[K] {
	return ef.allocator().Add(id)
}

func (ef *EmptyFilter[K]) Clear() Filter[K] {
	return ef
}

func (ef *EmptyFilter[K]) Copy() Filter[K] {
	return ef
}

func (ef *EmptyFilter[K]) Capacity() int {
	return 0
}

func (ef *EmptyFilter[K]) Count() int {
	return 0
}

func (ef *EmptyFilter[K]) Equal(other Filter[K]) bool {
	_, ok := other.(*EmptyFilter[K])
	return ok
}
