package carmirror

import (
	"errors"
	"math"
	"sync"

	"github.com/fission-codes/go-bloom"
)

type Side uint8

const (
	SIDE_A Side = 0
	SIDE_B Side = 1
)

var ErrIncompatibleFilter = errors.New("incompatible filter type")
var ErrBloomOverflow = errors.New("bloom filter overflowing")

type CompoundFilter[K comparable] struct {
	a Filter[K]
	b Filter[K]
}

func (cf *CompoundFilter[K]) DoesNotContain(id K) bool {
	return cf.a.DoesNotContain(id) && cf.b.DoesNotContain(id)
}

func (cf *CompoundFilter[K]) GetSparser() Side {
	// -1 indicates unlimited capacity, therefore automatically the sparser of two sides
	if cf.a.GetCapacity() < 0 || cf.b.GetCapacity() < 0 {
		if cf.a.GetCapacity() >= 0 {
			return SIDE_B
		}
	} else {
		// Otherwise find the side with the most unused capacity
		if cf.b.GetCapacity()-cf.b.GetCount() > cf.a.GetCapacity()-cf.a.GetCount() {
			return SIDE_B
		}
	}
	return SIDE_A
}

func (cf *CompoundFilter[K]) AddAll(other Filter[K]) Filter[K] {
	// TODO: Deal with case where other is a CompoundFilter

	// Try to add to the spareser of the two sides. If this fails,
	// try adding to the other side. If this fails, create a new node
	// on the less sparse side
	if cf.GetSparser() == SIDE_A {
		cf.a = cf.a.AddAll(other)
	} else {
		cf.b = cf.b.AddAll(other)
	}
	return cf
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
	if cf.a.GetCapacity() < 0 || cf.b.GetCapacity() > 0 && cf.a.GetCapacity() > cf.b.GetCapacity() {
		return cf.a.Clear()
	} else {
		return cf.b.Clear()
	}
}

func (cf *CompoundFilter[K]) Copy() Filter[K] {
	return &CompoundFilter[K]{cf.a.Copy(), cf.b.Copy()}
}

func (cf *CompoundFilter[K]) GetCapacity() int {
	if cf.a.GetCapacity() < 0 || cf.b.GetCapacity() < 0 {
		return -1
	} else {
		return cf.a.GetCapacity() + cf.b.GetCapacity()
	}
}

func (cf *CompoundFilter[K]) GetCount() int {
	return cf.a.GetCount() + cf.b.GetCount()
}

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

func (f *BloomFilter[K, H]) AddAll(other Filter[K]) Filter[K] {
	obf, ok := other.(*BloomFilter[K, H])
	if ok {
		if f.capacity < f.count+obf.count {
			working_bloom := f.filter.Copy()
			err := working_bloom.Union(obf.filter)
			if err == nil {
				new_count := int(f.filter.EstimateEntries())
				if new_count < f.capacity {
					f.filter = working_bloom
					f.count = new_count
					return f
				}
			}
		} else {
			err := f.filter.Union(obf.filter)
			if err == nil {
				f.count = int(f.filter.EstimateEntries())
				return f
			}
		}
	}
	return &CompoundFilter[K]{
		a: f,
		b: other.Copy(),
	}
}

func (f *BloomFilter[K, H]) GetCapacity() int {
	return f.capacity
}

func (f *BloomFilter[K, H]) GetCount() int {
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

func (rf *RootFilter[K]) GetCapacity() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.GetCapacity()
}

func (rf *RootFilter[K]) GetCount() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.GetCount()
}

type PerfectFilter[K comparable] struct {
	filter map[K]bool
}

func NewPerfectFilter[K comparable]() *PerfectFilter[K] {
	return &PerfectFilter[K]{make(map[K]bool)}
}

func (pf *PerfectFilter[K]) DoesNotContain(id K) bool {
	return !pf.filter[id]
}

func (pf *PerfectFilter[K]) AddAll(other Filter[K]) Filter[K] {
	opf, ok := other.(*PerfectFilter[K])
	if ok {
		for k := range opf.filter {
			pf.filter[k] = true
		}
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

func (pf *PerfectFilter[K]) GetCapacity() int {
	return -1 // capacity of perfect filter is unconstrained
}

func (pf *PerfectFilter[K]) GetCount() int {
	return len(pf.filter)
}
