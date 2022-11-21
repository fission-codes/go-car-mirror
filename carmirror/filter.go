package carmirror

import (
	"errors"
	"math"

	"github.com/fission-codes/go-bloom"
)

type Side uint8

const (
	SIDE_A Side = 0
	SIDE_B Side = 1
)

var ERR_INCOMPATIBLE_FILTER = errors.New("Incompatible filter type")
var ERR_BLOOM_OVERFLOW = errors.New("Bloom filter overflowing")

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

func (cf *CompoundFilter[K]) AddAll(other Filter[K]) error {
	// Try to add to the spareser of the two sides. If this fails,
	// try adding to the other side. If this fails, create a new node
	// on the less sparse side
	if cf.GetSparser() == SIDE_A {
		if cf.a.AddAll(other) != nil {
			if cf.b.AddAll(other) != nil {
				cf.b = &CompoundFilter[K]{
					a: other,
					b: cf.b,
				}
			}
		}
	} else {
		if cf.b.AddAll(other) != nil {
			if cf.a.AddAll(other) != nil {
				cf.a = &CompoundFilter[K]{
					a: cf.a,
					b: other,
				}
			}
		}
	}
	return nil
}

func (cf *CompoundFilter[K]) Add(id K) error {
	// Try to add to the sparser side. If this fails,
	// add to the other side.
	if cf.GetSparser() == SIDE_A {
		if cf.a.Add(id) != nil {
			return cf.b.Add(id)
		} else {
			return nil
		}
	} else {
		if cf.b.Add(id) != nil {
			return cf.a.Add(id)
		} else {
			return nil
		}
	}
}

func (cf *CompoundFilter[K]) Clear() error {
	err := cf.a.Clear()
	if err != nil {
		return err
	}
	return cf.b.Clear()
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
	filter, _ := bloom.NewFilter[K, H](bitCount, hashCount, function)

	return &BloomFilter[K, H]{
		filter:   filter,
		capacity: int(capacity),
		count:    0,
	}
}

func (f *BloomFilter[K, H]) Add(id K) error {
	if f.count < f.capacity {
		if !f.filter.Test(id) {
			f.count = f.count + 1
			f.filter.Add(id)
		}
		return nil
	} else {
		return ERR_BLOOM_OVERFLOW
	}
}

func (f *BloomFilter[K, H]) DoesNotContain(id K) bool {
	return !f.filter.Test(id)
}

func (f *BloomFilter[K, H]) AddAll(other Filter[K]) error {
	obf, ok := other.(*BloomFilter[K, H])
	if ok {
		if f.capacity >= f.count+obf.count {
			err := f.filter.Union(obf.filter)
			if err != nil {
				f.count = int(f.filter.EstimateEntries())
			}
			return err
		} else {
			return ERR_BLOOM_OVERFLOW
		}
	} else {
		return ERR_INCOMPATIBLE_FILTER
	}
}

func (f *BloomFilter[K, H]) GetCapacity() int {
	return f.capacity
}

func (f *BloomFilter[K, H]) GetCount() int {
	return f.count
}

func (f *BloomFilter[K, H]) Clear() error {
	filter, err := bloom.NewFilter[K, H](f.filter.BitCount(), f.filter.HashCount(), f.filter.HashFunction())
	if err == nil {
		f.filter = filter
	}
	return err
}

type FilterFactory[K comparable] func(uint) Filter[K]

type RootFilter[K comparable] struct {
	filter       Filter[K]
	factory      FilterFactory[K]
	min_capacity uint
}

func NewRootFilter[K comparable](min_capacity uint, factory FilterFactory[K]) *RootFilter[K] {
	return &RootFilter[K]{nil, factory, min_capacity}
}

func (rf *RootFilter[K]) DoesNotContain(id K) bool {
	return rf.filter == nil || rf.DoesNotContain(id)
}

func (rf *RootFilter[K]) AddAll(other Filter[K]) error {
	if rf.filter == nil {
		rf.filter = other
	} else {
		err := rf.filter.AddAll(other)
		if err != nil {
			rf.filter = &CompoundFilter[K]{rf.filter, other}
		}
	}
	return nil
}

func (rf *RootFilter[K]) Add(id K) error {
	if rf.filter == nil {
		rf.filter = rf.factory(rf.min_capacity)
	}
	err := rf.filter.Add(id)
	if err != nil {
		capacity_increment := rf.min_capacity
		if rf.filter.GetCapacity() > int(rf.min_capacity) {
			capacity_increment = uint(rf.filter.GetCapacity())
		}
		rf.filter = &CompoundFilter[K]{rf.filter, rf.factory(uint(capacity_increment))}
		err = rf.filter.Add(id)
	}
	return err
}

func (rf *RootFilter[K]) Clear() error {
	rf.filter = nil
	return nil
}

func (rf *RootFilter[K]) GetCapacity() int {
	return -1 // the capacity of a root filter is unconstrained
}

func (rf *RootFilter[K]) GetCount() int {
	if rf.filter == nil {
		return 0
	} else {
		return rf.filter.GetCount()
	}
}

type PerfectFilter[K comparable] struct {
	filter map[K]bool
}

func NewPerfectFilter[K comparable](min_capacity uint32, factory FilterFactory[K]) *PerfectFilter[K] {
	return &PerfectFilter[K]{make(map[K]bool)}
}

func (pf *PerfectFilter[K]) DoesNotContain(id K) bool {
	return !pf.filter[id]
}

func (pf *PerfectFilter[K]) AddAll(other Filter[K]) error {
	opf, ok := other.(*PerfectFilter[K])
	if ok {
		for k, _ := range opf.filter {
			pf.filter[k] = true
		}
		return nil
	} else {
		return ERR_INCOMPATIBLE_FILTER
	}
}

func (pf *PerfectFilter[K]) Add(id K) error {
	pf.filter[id] = true
	return nil
}

func (pf *PerfectFilter[K]) Clear() error {
	pf.filter = make(map[K]bool)
	return nil
}

func (pf *PerfectFilter[K]) GetCapacity() int {
	return -1 // capacity of perfect filter is unconstrained
}

func (pf *PerfectFilter[K]) GetCount() int {
	return len(pf.filter)
}
