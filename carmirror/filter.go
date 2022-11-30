package carmirror

import (
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

// A compound filter implements the union of two disparate filter types
type CompoundFilter[K comparable] struct {
	a Filter[K]
	b Filter[K]
}

func (cf *CompoundFilter[K]) DoesNotContain(id K) bool {
	return cf.a.DoesNotContain(id) && cf.b.DoesNotContain(id)
}

func (cf *CompoundFilter[K]) GetSparser() Side {
	// -1 indicates unlimited capacity, therefore automatically the sparser of two sides
	if cf.a.Capacity() < 0 || cf.b.Capacity() < 0 {
		if cf.a.Capacity() >= 0 {
			return SIDE_B
		}
	} else {
		// Otherwise find the side with the most unused capacity
		if cf.b.Capacity()-cf.b.Count() > cf.a.Capacity()-cf.a.Count() {
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
		err = cf.a.TryAddAll(other)
	} else {
		err = cf.b.TryAddAll(other)
	}

	if err != nil {
		if cf.GetSparser() == SIDE_A {
			err = cf.b.TryAddAll(other)
		} else {
			err = cf.a.TryAddAll(other)
		}
	}
	return err
}

func (cf *CompoundFilter[K]) AddAll(other Filter[K]) Filter[K] {
	ocf, ok := other.(*CompoundFilter[K])
	if ok {
		return cf.AddAll(ocf.a).AddAll(ocf.b)
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
		cf.a = cf.a.Add(id)
	} else {
		cf.b = cf.b.Add(id)
	}
	return cf
}

func (cf *CompoundFilter[K]) Clear() Filter[K] {
	if cf.a.Capacity() < 0 || cf.b.Capacity() > 0 && cf.a.Capacity() > cf.b.Capacity() {
		return cf.a.Clear()
	} else {
		return cf.b.Clear()
	}
}

func (cf *CompoundFilter[K]) Copy() Filter[K] {
	return &CompoundFilter[K]{cf.a.Copy(), cf.b.Copy()}
}

func (cf *CompoundFilter[K]) Capacity() int {
	if cf.a.Capacity() < 0 || cf.b.Capacity() < 0 {
		return -1
	} else {
		return cf.a.Capacity() + cf.b.Capacity()
	}
}

func (cf *CompoundFilter[K]) Count() int {
	return cf.a.Count() + cf.b.Count()
}

// A bloom filter is a classic, fixed-capacity bloom filter which also stores an approximate count of the number of entries
type BloomFilter[K comparable, H bloom.HashFunction[K]] struct {
	filter   *bloom.Filter[K, H]
	capacity int
	count    int
}

// TODO: Add New* methods to mirror those in bloom.Filter
func NewBloomFilter[K comparable, H bloom.HashFunction[K]](capacity uint, function H) *BloomFilter[K, H] {
	if capacity > math.MaxInt {
		capacity = math.MaxInt
	}
	bitCount, hashCount := bloom.EstimateParameters(uint64(capacity), bloom.EstimateFPP(uint64(capacity)))
	filter, _ := bloom.NewFilter[K](bitCount, hashCount, function)

	return &BloomFilter[K, H]{
		filter:   filter,
		capacity: int(capacity),
		count:    0,
	}
}

// When adding a new item would saturate the filter, create a compound filter composed of the existing filter plus a new bloom of double the capacity
func (f *BloomFilter[K, H]) Add(id K) Filter[K] {
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
			a: f,
			b: extension,
		}
	}
}

func (f *BloomFilter[K, H]) DoesNotContain(id K) bool {
	return !f.filter.Test(id)
}

func (f *BloomFilter[K, H]) TryAddAll(other Filter[K]) error {
	obf, ok := other.(*BloomFilter[K, H])
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

func (f *BloomFilter[K, H]) AddAll(other Filter[K]) Filter[K] {
	err := f.TryAddAll(other)
	if err == nil {
		return f
	} else {
		return &CompoundFilter[K]{
			a: f,
			b: other.Copy(),
		}
	}
}

func (f *BloomFilter[K, H]) Capacity() int {
	return f.capacity
}

func (f *BloomFilter[K, H]) Count() int {
	return f.count
}

func (f *BloomFilter[K, H]) Clear() Filter[K] {
	return NewBloomFilter[K](uint(f.capacity), f.filter.HashFunction())
}

func (f *BloomFilter[K, H]) Copy() Filter[K] {
	return &BloomFilter[K, H]{
		filter:   f.filter.Copy(),
		capacity: f.capacity,
		count:    f.count,
	}
}

// A thread-safe Filter which wraps a regular Filter with a mutex.
type RootFilter[K comparable] struct {
	filter Filter[K]
	lock   sync.RWMutex
}

func NewRootFilter[K comparable](filter Filter[K]) *RootFilter[K] {
	return &RootFilter[K]{filter: filter, lock: sync.RWMutex{}}
}

func (rf *RootFilter[K]) DoesNotContain(id K) bool {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.DoesNotContain(id)
}

func (rf *RootFilter[K]) TryAddAll(other Filter[K]) error {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	return rf.filter.TryAddAll(other)
}

func (rf *RootFilter[K]) AddAll(other Filter[K]) Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.AddAll(other)
	return rf
}

func (rf *RootFilter[K]) Add(id K) Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.Add(id)
	return rf
}

func (rf *RootFilter[K]) Clear() Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.Clear()
	return rf
}

func (rf *RootFilter[K]) Copy() Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	return &RootFilter[K]{filter: rf.filter.Copy(), lock: sync.RWMutex{}}
}

func (rf *RootFilter[K]) Capacity() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.Capacity()
}

func (rf *RootFilter[K]) Count() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.Count()
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
		return &CompoundFilter[K]{a: pf, b: other.Copy()}
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
