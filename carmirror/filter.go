package carmirror

import "github.com/fission-codes/go-bloom"

type Side uint8

const (
	SIDE_A Side = 0
	SIDE_B Side = 1
)

type CompoundFilter[K comparable] struct {
	a Filter[K]
	b Filter[K]
}

func (cf *CompoundFilter[K]) DoesNotContain(id K) bool {
	return cf.a.DoesNotContain(id) && cf.b.DoesNotContain(id)
}

func (cf *CompoundFilter[K]) GetSparser() Side {
	// Zero indicates unlimited capacity, therefore automatically the sparser of two sides
	if cf.a.GetCapacity() == 0 || cf.b.GetCapacity() == 0 {
		if cf.a.GetCapacity() != 0 {
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

func (cf *CompoundFilter[K]) GetCapacity() uint32 {
	return cf.a.GetCapacity() + cf.b.GetCapacity()
}

func (cf *CompoundFilter[K]) GetCount() uint32 {
	return cf.a.GetCount() + cf.b.GetCount()
}

type BloomFilter[K comparable, H bloom.HashFunction[K]] struct {
	filter *bloom.Filter[K, H]
}

// TODO: Add New* methods to mirror those in bloom.Filter
func NewBloomFilter[K comparable, H bloom.HashFunction[K]](bitCount uint64, hashCount uint64, function H) *BloomFilter[K, H] {
	filter, _ := bloom.NewFilter[K, H](bitCount, hashCount, function)

	return &BloomFilter[K, H]{
		filter: filter,
	}
}

func (f *BloomFilter[K, H]) Add(id K) error {
	f.filter.Add(id)

	return nil
}

func (f *BloomFilter[K, H]) DoesNotContain(id K) bool {
	return !f.filter.Test(id)
}

func (f *BloomFilter[K, H]) AddAll(other *BloomFilter[K, H]) error {
	return f.filter.Union(other.filter)
}

type FilterFactory[K comparable] func(uint32) Filter[K]

type RootFilter[K comparable] struct {
	filter  Filter[K]
	factory FilterFactory[K]
}

func (rf *RootFilter[K]) DoesNotContain(id K) bool {
	return rf.filter == nil || rf.DoesNotContain(id)
}

func (rf *RootFilter[K]) AddAll(other Filter[K]) error {
	if rf.filter == nil {
		rf.filter = other
	} else {
		err := rf.AddAll(other)
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
	err := rf.Add(id)
	if err != nil {
		rf.filter = &CompoundFilter[K]{rf.filter, other}
	}
	return nil
}

func (rf *RootFilter[K]) Clear() error {
	rf.filter = nil
}

func (rf *RootFilter[K]) GetCapacity() uint32 {
	if ref.filter == nil {
		return 0
	} else {
		return rf.filter.GetCapacity()
	}
}

func (rf *RootFilter[K]) GetCount() uint32 {
	if ref.filter == nil {
		return 0
	} else {
		return rf.filter.GetCount()
	}
}
