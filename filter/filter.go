// Package filter provides a set of generic filter implementations that can be used to track any comparable.  For purposes of go-car-mirror, they are used to track carmirror.BlockId.
//
// The name filter is inspired by bloom filters and the related family of space-efficient, probabilistic data structures that can be used to test whether an element is a member of a set.
// The test is probabilistic in that it may return false positives, but will never return a false negative.
package filter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/fission-codes/go-bloom"
	cbor "github.com/fxamacker/cbor/v2"
	golog "github.com/ipfs/go-log/v2"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

var log = golog.Logger("filter")

// Filter is a space-efficient, probabilistic data structure that can be used to test whether an element is a member of a set.
// The test is probabilistic in that it may return false positives, but will never return a false negative.
type Filter[K comparable] interface {
	// DoesNotContain returns true if item is not present in the filter.
	DoesNotContain(item K) bool

	// Capacity returns the total number of items this filter can contain.
	// May return -1 to indicate unconstrained.
	Capacity() int

	// Count returns an estimate of the number of items in the filter.
	Count() int

	// Copy returns a copy or reference to the filter, as appropriate.
	Copy() Filter[K]

	// Equal returns true if the two filters are equivalent.
	Equal(Filter[K]) bool

	// Add adds the two filters together.
	// Mutate operations follow the Go convention of normally mutating, but returning a new filter when necessary.
	Add(item K) Filter[K]

	// Clear removes all items from the filter.
	Clear() Filter[K]

	// TryAddAll attempts to add all items from other filter to this filter.
	// It fails if the other filter cannot be added.
	TryAddAll(other Filter[K]) error

	// AddAll adds all items from the other filter to this filter.
	// It creates a new filter, if necessary, to accomodate the merge.
	AddAll(other Filter[K]) Filter[K]

	// Dump dumps the filter data to the log with the given prefix.
	// Note: this may be quite slow, as there are multiple lines of output.
	Dump(*zap.SugaredLogger, string)
}

// FilterWireFormat is a wire format for filters.
// It is used to serialize and deserialize filters and can support multiple filter types.
type FilterWireFormat[K comparable] struct {
	// BL is a Bloom filter.
	BL *BloomFilter[K] `json:"bl,omitempty"`
	// CM is a compound filter.
	CM *CompoundFilter[K] `json:"cm,omitempty"`
	// EM is an empty filter.
	EM *EmptyFilter[K] `json:"em,omitempty"`
	// PF is a perfect filter, which is mainly used for testing.
	PF *PerfectFilter[K] `json:"pf,omitempty"`
}

// Any returns the first non-nil filter in the wire format.
func (wf *FilterWireFormat[K]) Any() Filter[K] {
	if wf.BL != nil {
		return wf.BL
	}
	if wf.CM != nil {
		return wf.CM
	}
	if wf.EM != nil {
		return wf.EM
	}
	if wf.PF != nil {
		return wf.PF
	}
	return nil
}

// NewFilterWireFormat creates a new wire format for the given filter.
func NewFilterWireFormat[K comparable](filter Filter[K]) *FilterWireFormat[K] {
	if bl, ok := filter.(*BloomFilter[K]); ok {
		return &FilterWireFormat[K]{BL: bl}
	}
	if cm, ok := filter.(*CompoundFilter[K]); ok {
		return &FilterWireFormat[K]{CM: cm}
	}
	if em, ok := filter.(*EmptyFilter[K]); ok {
		return &FilterWireFormat[K]{EM: em}
	}
	if pf, ok := filter.(*PerfectFilter[K]); ok {
		return &FilterWireFormat[K]{PF: pf}
	}
	if pf, ok := filter.(*SynchronizedFilter[K]); ok {
		return NewFilterWireFormat(pf.UnsynchronizedCopy())
	}
	return nil
}

type Side uint8

const (
	SIDE_A Side = 0
	SIDE_B Side = 1
)

// Errors that can be returned for invalid filter operations.
var (
	ErrIncompatibleFilter            = errors.New("incompatible filter type")
	ErrBloomOverflow                 = errors.New("bloom filter overflowing")
	ErrCantMarshalBloomWithoutHashId = errors.New("bloom must be created with a hash Id in order to marshal it")
)

// CompoundFilter implements the union of two disparate filter types
type CompoundFilter[K comparable] struct {
	SideA Filter[K]
	SideB Filter[K]
}

// DoesNotContain returns true if item is not present in the filter.
func (cf *CompoundFilter[K]) DoesNotContain(item K) bool {
	return cf.SideA.DoesNotContain(item) && cf.SideB.DoesNotContain(item)
}

// GetSparser returns the side of the filter with the most unused capacity.
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

// TryAddAll attempts to add all items from other filter to this filter.
func (cf *CompoundFilter[K]) TryAddAll(other Filter[K]) error {
	other = Desynchronize(other)
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

// AddAll adds all items from the other filter to this filter.
func (cf *CompoundFilter[K]) AddAll(other Filter[K]) Filter[K] {
	other = Desynchronize(other)
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

// Add adds an item to the sparser side of the filter.
func (cf *CompoundFilter[K]) Add(item K) Filter[K] {
	// Try to add to the sparser side. If this fails,
	// add to the other side.
	if cf.GetSparser() == SIDE_A {
		cf.SideA = cf.SideA.Add(item)
	} else {
		cf.SideB = cf.SideB.Add(item)
	}
	return cf
}

// Clear returns a cleared filter with the same type as the sparser side.
func (cf *CompoundFilter[K]) Clear() Filter[K] {
	if cf.SideA.Capacity() < 0 || cf.SideB.Capacity() > 0 && cf.SideA.Capacity() > cf.SideB.Capacity() {
		return cf.SideA.Clear()
	} else {
		return cf.SideB.Clear()
	}
}

// Copy returns a copy of the filter.
func (cf *CompoundFilter[K]) Copy() Filter[K] {
	return &CompoundFilter[K]{cf.SideA.Copy(), cf.SideB.Copy()}
}

// Capacity returns the total capacity of the filter.
func (cf *CompoundFilter[K]) Capacity() int {
	if cf.SideA.Capacity() < 0 || cf.SideB.Capacity() < 0 {
		return -1
	} else {
		return cf.SideA.Capacity() + cf.SideB.Capacity()
	}
}

// Count returns the number of items in the filter.
func (cf *CompoundFilter[K]) Count() int {
	return cf.SideA.Count() + cf.SideB.Count()
}

func (cf *CompoundFilter[K]) Equal(other Filter[K]) bool {
	ocf, ok := other.(*CompoundFilter[K])
	return ok && cf.SideA.Equal(ocf.SideA) && cf.SideB.Equal(ocf.SideB)
}

// UnmarshalJSON unmarshals the JSON-encoded CompoundFilterWireFormat into the filter.
func (cf *CompoundFilter[K]) UnmarshalJSON(bytes []byte) error {
	var wireFormat CompoundFilterWireFormat[K] = CompoundFilterWireFormat[K]{}
	err := json.Unmarshal(bytes, &wireFormat)
	if err == nil {
		cf.SideA = wireFormat.SideA.Any()
		cf.SideB = wireFormat.SideB.Any()
	}
	return err
}

// UnmarshalCBOR unmarshals the CBOR-encoded CompoundFilterWireFormat into the filter.
func (cf *CompoundFilter[K]) UnmarshalCBOR(bytes []byte) error {
	var wireFormat CompoundFilterWireFormat[K] = CompoundFilterWireFormat[K]{}
	err := cbor.Unmarshal(bytes, &wireFormat)
	if err == nil {
		cf.SideA = wireFormat.SideA.Any()
		cf.SideB = wireFormat.SideB.Any()
	}
	return err
}

// MarshalJSON marshals the filter into a JSON-encoded CompoundFilterWireFormat.
func (cf *CompoundFilter[K]) MarshalJSON() ([]byte, error) {
	var wireFormat CompoundFilterWireFormat[K] = CompoundFilterWireFormat[K]{
		SideA: NewFilterWireFormat(cf.SideA),
		SideB: NewFilterWireFormat(cf.SideB),
	}
	return json.Marshal(wireFormat)
}

// MarshalCBOR marshals the filter into a CBOR-encoded CompoundFilterWireFormat.
func (cf *CompoundFilter[K]) MarshalCBOR() ([]byte, error) {
	var wireFormat CompoundFilterWireFormat[K] = CompoundFilterWireFormat[K]{
		SideA: NewFilterWireFormat(cf.SideA),
		SideB: NewFilterWireFormat(cf.SideB),
	}
	return cbor.Marshal(wireFormat)
}

// Dump prints a human-readable representation of the filter to the log.
func (cf *CompoundFilter[K]) Dump(log *zap.SugaredLogger, prefix string) {
	log.Debugf("%sCompoundFilter {", prefix)
	cf.SideA.Dump(log, prefix+"  ")
	cf.SideB.Dump(log, prefix+"  ")
	log.Debugf("%s}", prefix)
}

// CompoundFilterWireFormat is the wire format for a CompoundFilter.
type CompoundFilterWireFormat[K comparable] struct {
	SideA *FilterWireFormat[K] `json:"a"`
	SideB *FilterWireFormat[K] `json:"b"`
}

// BloomFilter is a classic, fixed-capacity bloom filter, which also stores an approximate count of the number of items.
type BloomFilter[K comparable] struct {
	filter       *bloom.Filter[K]
	capacity     int
	count        int
	hashFunction uint64
}

// NewBloomFilter creates a bloom filter with the given estimated capacity.
// The false positive rate is determined by bloom.EstimateFPP.
func NewBloomFilter[K comparable](capacity uint, function bloom.HashFunction[K]) *BloomFilter[K] {
	if capacity > math.MaxInt {
		capacity = math.MaxInt
	}
	bitCount, hashCount := bloom.EstimateParameters(uint64(capacity), bloom.EstimateFPP(uint64(capacity)))
	filter, _ := bloom.NewFilter(bitCount, hashCount, function)

	log.Debugw("BloomFilter", "method", "NewBloomFilter", "bitCount", bitCount, "hashCount", hashCount)

	return &BloomFilter[K]{
		filter:       filter,
		capacity:     int(capacity),
		count:        0,
		hashFunction: 0,
	}
}

// NewBloomFilterFromBytes creates a bloom filter from a prepopulated byte array and related low-level information including the hash function.
func NewBloomFilterFromBytes[K comparable](bytes []byte, bitCount uint64, hashCount uint64, function bloom.HashFunction[K]) *BloomFilter[K] {
	filter := bloom.NewFilterFromBloomBytes[K](bitCount, hashCount, bytes, function)

	log.Debugw("BloomFilter", "method", "NewBloomFilterFromBytes", "bitCount", bitCount, "hashCount", hashCount)

	return &BloomFilter[K]{
		filter:       filter,
		capacity:     int(filter.EstimateCapacity()),
		count:        int(filter.EstimateEntries()),
		hashFunction: 0,
	}
}

// TryNewBloomFilter creates a bloom filter of the given capacity using the registered hash function, failing if no such hash function can be found.
func TryNewBloomFilter[K comparable](capacity uint, function uint64) (*BloomFilter[K], error) {
	if hashFunction, ok := RegistryLookup[K](function); ok {
		filter := NewBloomFilter(capacity, hashFunction)
		filter.hashFunction = function
		return filter, nil
	} else {
		return nil, ErrIncompatibleFilter
	}
}

// TryNewBloomFilterFromBytes creates a bloom filter from a prepopulated byte array and related low-level information including the hash function, failing if no such hash function can be found.
func TryNewBloomFilterFromBytes[K comparable](bytes []byte, bitCount uint64, hashCount uint64, function uint64) (*BloomFilter[K], error) {
	if hashFunction, ok := RegistryLookup[K](function); ok {
		filter := NewBloomFilterFromBytes(bytes, bitCount, hashCount, hashFunction)
		filter.hashFunction = function
		return filter, nil
	} else {
		return nil, ErrIncompatibleFilter
	}
}

// Add adds an item to the filter.
// When adding a new item would saturate the filter, it creates a compound filter composed of the existing filter plus a new bloom of double the capacity.
func (f *BloomFilter[K]) Add(item K) Filter[K] {
	if f.count < f.capacity {
		if !f.filter.Test(item) {
			f.count = f.count + 1
			f.filter.Add(item)
		}
		return f
	} else {
		var extension Filter[K]
		var err error
		if f.hashFunction != 0 {
			// use the hash function Id if we have one
			extension, err = TryNewBloomFilter[K](uint(f.capacity)*2, f.hashFunction)
		}
		if f.hashFunction == 0 || err != nil {
			// otherwise fall back to using the hash function pointer from the bloom itself
			extension = NewBloomFilter(uint(f.capacity)*2, f.filter.HashFunction())
		}
		extension = extension.Add(item)
		return &CompoundFilter[K]{
			SideA: f,
			SideB: extension,
		}
	}
}

// DoesNotContain returns true if the item is not in the filter.
func (f *BloomFilter[K]) DoesNotContain(item K) bool {
	return !f.filter.Test(item)
}

// TryAddAll attempts to add all items from another filter to this one.
func (f *BloomFilter[K]) TryAddAll(other Filter[K]) error {
	other = Desynchronize(other)
	obf, ok := other.(*BloomFilter[K])
	if ok {
		log.Debugw("enter", "object", "BloomFilter", "method", "TryAddAll", "other.bitCount", obf.filter.BitCount(), "hashCount", obf.filter.HashCount())
		if f.capacity < f.count+obf.count {
			working_bloom := f.filter.Copy()
			err := working_bloom.Union(obf.filter)
			if err != nil {
				log.Debugw("exit", "object", "BloomFilter", "method", "TryAddAll", "error", err)
				return err
			}
			new_count := int(working_bloom.EstimateEntries())
			if new_count >= f.capacity {
				log.Debugw("exit", "object", "BloomFilter", "method", "TryAddAll", "error", ErrBloomOverflow)
				return ErrBloomOverflow
			}
			f.filter = working_bloom
			f.count = new_count
		} else {
			err := f.filter.Union(obf.filter)
			if err != nil {
				log.Debugw("exit", "object", "BloomFilter", "method", "TryAddAll", "error", err)
				return err
			}
			f.count = int(f.filter.EstimateEntries())
		}
		log.Debugw("exit", "object", "BloomFilter", "method", "TryAddAll", "error", nil)
		return nil
	} else if _, ok := other.(*EmptyFilter[K]); ok {
		log.Debugw("exit", "object", "BloomFilter", "method", "TryAddAll", "error", nil)

		return nil
	} else {
		log.Debugw("exit", "object", "BloomFilter", "method", "TryAddAll", "error", ErrIncompatibleFilter)
		return ErrIncompatibleFilter
	}
}

// AddAll adds all items from another filter to this one.
func (f *BloomFilter[K]) AddAll(other Filter[K]) Filter[K] {
	other = Desynchronize(other)
	err := f.TryAddAll(other)
	if err == nil {
		return f
	} else {
		if ocf, ok := other.(*CompoundFilter[K]); ok {
			return ocf.Copy().AddAll(f)
		} else {
			return &CompoundFilter[K]{
				SideA: f,
				SideB: other.Copy(),
			}
		}
	}
}

// Capacity returns the maximum number of items the filter can hold.
func (f *BloomFilter[K]) Capacity() int {
	return f.capacity
}

// Count returns the number of items in the filter.
func (f *BloomFilter[K]) Count() int {
	return f.count
}

// Clear returns a new filter with the same capacity and hash function as the current one, but with no items.
func (f *BloomFilter[K]) Clear() Filter[K] {
	if f.hashFunction == 0 {
		return NewBloomFilter(uint(f.capacity), f.filter.HashFunction())
	} else {
		empty, err := TryNewBloomFilter[K](uint(f.capacity), f.hashFunction)
		if err != nil {
			panic(err)
		}
		return empty
	}
}

// Copy returns a copy of the filter.
func (f *BloomFilter[K]) Copy() Filter[K] {
	return &BloomFilter[K]{
		filter:       f.filter.Copy(),
		capacity:     f.capacity,
		count:        f.count,
		hashFunction: f.hashFunction,
	}
}

// Equal returns true if the other filter is a BloomFilter with the same parameters and items.
func (bf *BloomFilter[K]) Equal(other Filter[K]) bool {
	obf, ok := other.(*BloomFilter[K])
	return ok &&
		bytes.Equal(bf.filter.Bytes(), obf.filter.Bytes()) &&
		bf.filter.HashCount() == obf.filter.HashCount() &&
		bf.filter.BitCount() == obf.filter.BitCount() &&
		bf.hashFunction == obf.hashFunction
}

// BloomWireFormat is the wire format for a BloomFilter.
type BloomWireFormat[K comparable] struct {
	Bytes        []byte `json:"bytes"`
	HashFunction uint64 `json:"hashFunction"`
	HashCount    uint64 `json:"hashCount"`
	BitCount     uint64 `json:"bitCount"`
}

// UnmarshalTo unmarshals the wire format into a BloomFilter.
// If the hash function is not registered, it returns ErrIncompatibleFilter.
func (wf *BloomWireFormat[K]) UnmarshalTo(target *BloomFilter[K]) error {
	if hashFunction, ok := RegistryLookup[K](wf.HashFunction); ok {
		target.filter = bloom.NewFilterFromBloomBytes(wf.BitCount, wf.HashCount, wf.Bytes, hashFunction)
		target.capacity = int(target.filter.EstimateCapacity())
		target.count = int(target.filter.EstimateEntries())
		target.hashFunction = wf.HashFunction
		return nil
	} else {
		return ErrIncompatibleFilter
	}
}

// UnmarshalJSON unmarshals the JSON-encoded wire format into a BloomFilter.
func (bf *BloomFilter[K]) UnmarshalJSON(bytes []byte) error {
	wireFormat := BloomWireFormat[K]{}
	err := json.Unmarshal(bytes, &wireFormat)
	if err == nil {
		err = wireFormat.UnmarshalTo(bf)
	}
	return err
}

// UnmarshalCBOR unmarshals the CBOR-encoded wire format into a BloomFilter.
func (bf *BloomFilter[K]) UnmarshalCBOR(bytes []byte) error {
	wireFormat := BloomWireFormat[K]{}
	err := cbor.Unmarshal(bytes, &wireFormat)
	if err == nil {
		err = wireFormat.UnmarshalTo(bf)
	}
	return err
}

// MarshalJSON marshals the BloomFilter into a JSON-encoded wire format.
// If the hash function is not registered, it returns ErrCantMarshalBloomWithoutHashId.
func (bf *BloomFilter[K]) MarshalJSON() ([]byte, error) {
	if bf.hashFunction != 0 {
		return json.Marshal(&BloomWireFormat[K]{bf.filter.Bytes(), bf.hashFunction, bf.filter.HashCount(), bf.filter.BitCount()})
	} else {
		return nil, ErrCantMarshalBloomWithoutHashId
	}
}

// MarshalCBOR marshals the BloomFilter into a CBOR-encoded wire format.
func (bf *BloomFilter[K]) MarshalCBOR() ([]byte, error) {
	if bf.hashFunction != 0 {
		return cbor.Marshal(&BloomWireFormat[K]{bf.filter.Bytes(), bf.hashFunction, bf.filter.HashCount(), bf.filter.BitCount()})
	} else {
		return nil, ErrCantMarshalBloomWithoutHashId
	}
}

// Dump dumps the BloomFilter to the log.
func (bf *BloomFilter[K]) Dump(log *zap.SugaredLogger, prefix string) {
	log.Debugw(fmt.Sprintf("%sBloomFilter", prefix), "capacity", bf.capacity, "count", bf.count, "size", bf.filter.BitCount())
}

// SyncFilter is a Filter that is a thread-safe Filter, which wraps a regular Filter with a mutex.
// All of its methods are safe for concurrent use.
type SynchronizedFilter[K comparable] struct {
	filter Filter[K]
	lock   sync.RWMutex
}

// NewSynchronizedFilter returns a new SynchronizedFilter.
func NewSynchronizedFilter[K comparable](filter Filter[K]) *SynchronizedFilter[K] {
	return &SynchronizedFilter[K]{filter: filter, lock: sync.RWMutex{}}
}

// DoesNotContain returns true if the item is not in the filter.
func (rf *SynchronizedFilter[K]) DoesNotContain(item K) bool {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.DoesNotContain(item)
}

// TryAddAll adds all items from the other filter to the filter.
func (rf *SynchronizedFilter[K]) TryAddAll(other Filter[K]) error {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	return rf.filter.TryAddAll(other)
}

// AddAll adds all items from the other filter to the filter.
func (rf *SynchronizedFilter[K]) AddAll(other Filter[K]) Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.AddAll(other)
	return rf
}

// Add adds the item to the filter.
func (rf *SynchronizedFilter[K]) Add(item K) Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.Add(item)
	return rf
}

// Clear clears the filter.
func (rf *SynchronizedFilter[K]) Clear() Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.filter = rf.filter.Clear()
	return rf
}

// UnsynchronizedCopy returns an unsynchronized copy of the filter.
func (rf *SynchronizedFilter[K]) UnsynchronizedCopy() Filter[K] {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	return rf.filter.Copy()
}

// Copy returns a copy of the filter.
func (rf *SynchronizedFilter[K]) Copy() Filter[K] {
	return &SynchronizedFilter[K]{filter: rf.UnsynchronizedCopy(), lock: sync.RWMutex{}}
}

// Capacity returns the capacity of the filter.
func (rf *SynchronizedFilter[K]) Capacity() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.Capacity()
}

// Count returns the number of items in the filter.
func (rf *SynchronizedFilter[K]) Count() int {
	rf.lock.RLock()
	defer rf.lock.RUnlock()
	return rf.filter.Count()
}

// Equal returns true if the other filter is equal to the filter.
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

// UnmarshalJSON unmarshals the JSON-encoded wire format into the SynchronizedFilter.
func (sf *SynchronizedFilter[K]) UnmarshalJSON(bytes []byte) error {
	wireFormat := FilterWireFormat[K]{}
	err := json.Unmarshal(bytes, &wireFormat)
	if err == nil {
		sf.lock.Lock()
		defer sf.lock.Unlock()
		sf.filter = wireFormat.Any()
	}
	return err
}

// UnmarshalCBOR unmarshals the CBOR-encoded wire format into the SynchronizedFilter.
func (sf *SynchronizedFilter[K]) UnmarshalCBOR(bytes []byte) error {
	wireFormat := FilterWireFormat[K]{}
	err := cbor.Unmarshal(bytes, &wireFormat)
	if err == nil {
		sf.lock.Lock()
		defer sf.lock.Unlock()
		sf.filter = wireFormat.Any()
	}
	return err
}

// MarshalJSON marshals the SynchronizedFilter into the JSON-encoded wire format.
func (sf *SynchronizedFilter[K]) MarshalJSON() ([]byte, error) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	return json.Marshal(NewFilterWireFormat(sf.filter))
}

// MarshalCBOR marshals the SynchronizedFilter into the CBOR-encoded wire format.
func (sf *SynchronizedFilter[K]) MarshalCBOR() ([]byte, error) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	return cbor.Marshal(NewFilterWireFormat(sf.filter))
}

// Dump dumps the filter to the log.
func (sf *SynchronizedFilter[K]) Dump(log *zap.SugaredLogger, prefix string) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	log.Debugf("%sSynchronizedFilter {", prefix)
	sf.filter.Dump(log, prefix+"  ")
	log.Debugf("%s}", prefix)
}

// Desynchronize returns a filter which is not synchronized.
func Desynchronize[K comparable](filter Filter[K]) Filter[K] {
	if sf, ok := filter.(*SynchronizedFilter[K]); ok {
		return sf.UnsynchronizedCopy()
	} else {
		return filter
	}
}

// PerfectFilter is a Filter implementation which uses and underlying map - mainly for testing
type PerfectFilter[K comparable] struct {
	filter map[K]bool
}

// NewPerfectFilter returns a new PerfectFilter.
func NewPerfectFilter[K comparable]() *PerfectFilter[K] {
	return &PerfectFilter[K]{make(map[K]bool)}
}

// DoesNotContain returns true if the item is not in the filter.
func (pf *PerfectFilter[K]) DoesNotContain(item K) bool {
	return !pf.filter[item]
}

// TryAddAll adds all items from the other filter to the filter.
func (pf *PerfectFilter[K]) TryAddAll(other Filter[K]) error {
	other = Desynchronize(other)
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

// AddAll adds all items from the other filter to the filter.
func (pf *PerfectFilter[K]) AddAll(other Filter[K]) Filter[K] {
	other = Desynchronize(other)
	err := pf.TryAddAll(other)
	if err == nil {
		return pf
	} else {
		return &CompoundFilter[K]{SideA: pf, SideB: other.Copy()}
	}
}

// Add adds the item to the filter.
func (pf *PerfectFilter[K]) Add(item K) Filter[K] {
	pf.filter[item] = true
	return pf
}

// Clear clears the filter.
func (pf *PerfectFilter[K]) Clear() Filter[K] {
	pf.filter = make(map[K]bool)
	return pf
}

// Copy returns a copy of the filter.
func (pf *PerfectFilter[K]) Copy() Filter[K] {
	return NewPerfectFilter[K]().AddAll(pf)
}

// Capacity returns the capacity of the filter.
func (pf *PerfectFilter[K]) Capacity() int {
	return -1 // capacity of perfect filter is unconstrained
}

// Count returns the number of items in the filter.
func (pf *PerfectFilter[K]) Count() int {
	return len(pf.filter)
}

// Equal returns true if the other filter is equal to the filter.
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

// UnmarshalJSON unmarshals the JSON-encoded wire format into the PerfectFilter.
func (pf *PerfectFilter[K]) UnmarshalJSON(bytes []byte) error {
	var wireFormat []K
	err := json.Unmarshal(bytes, &wireFormat)
	if err == nil {
		pf.filter = make(map[K]bool, len(wireFormat))
		for _, key := range wireFormat {
			pf.filter[key] = true
		}
	}
	return err
}

// UnmarshalCBOR unmarshals the CBOR-encoded wire format into the PerfectFilter.
func (pf *PerfectFilter[K]) UnmarshalCBOR(bytes []byte) error {
	var wireFormat []K
	err := cbor.Unmarshal(bytes, &wireFormat)
	if err == nil {
		pf.filter = make(map[K]bool, len(wireFormat))
		for _, key := range wireFormat {
			pf.filter[key] = true
		}
	}
	return err
}

// MarshalJSON marshals the PerfectFilter into the JSON-encoded wire format.
func (pf *PerfectFilter[K]) MarshalJSON() ([]byte, error) {
	return json.Marshal(maps.Keys(pf.filter))
}

// MarshalCBOR marshals the PerfectFilter into the CBOR-encoded wire format.
func (pf *PerfectFilter[K]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal(maps.Keys(pf.filter))
}

// Dump dumps the filter to the log.
func (pf *PerfectFilter[K]) Dump(log *zap.SugaredLogger, prefix string) {
	log.Debugw(fmt.Sprintf("%sPerfectFilter", prefix), "length", len(pf.filter))
}

// EmptyFilter represents a filter which contains no items. An allocator is provided which is used to create a new filter if necessary.
type EmptyFilter[K comparable] struct {
	allocator func() Filter[K] `json:"-"`
}

// NewEmptyFilter returns a new EmptyFilter.
func NewEmptyFilter[K comparable](allocator func() Filter[K]) *EmptyFilter[K] {
	return &EmptyFilter[K]{allocator}
}

// DoesNotContain returns true if the item is not in the filter.
func (ef *EmptyFilter[K]) DoesNotContain(item K) bool {
	return true
}

// TryAddAll adds all items from the other filter to the filter.
// Returns an error if the other filter is not compatible with the filter.
func (ef *EmptyFilter[K]) TryAddAll(other Filter[K]) error {
	return ErrIncompatibleFilter
}

// AddAll adds all items from the other filter to the filter.
func (ef *EmptyFilter[K]) AddAll(other Filter[K]) Filter[K] {
	return other.Copy()
}

// Add adds the item to the filter.
func (ef *EmptyFilter[K]) Add(item K) Filter[K] {
	return ef.allocator().Add(item)
}

// Clear clears the filter.
func (ef *EmptyFilter[K]) Clear() Filter[K] {
	return ef
}

// Copy returns a copy of the filter.
func (ef *EmptyFilter[K]) Copy() Filter[K] {
	return ef
}

// Capacity returns the capacity of the filter.
func (ef *EmptyFilter[K]) Capacity() int {
	return 0
}

// Count returns the number of items in the filter.
func (ef *EmptyFilter[K]) Count() int {
	return 0
}

// Equal returns true if the other filter is equal to the filter.
func (ef *EmptyFilter[K]) Equal(other Filter[K]) bool {
	_, ok := other.(*EmptyFilter[K])
	return ok
}

// Dump dumps the filter to the log.
func (ef *EmptyFilter[K]) Dump(log *zap.SugaredLogger, prefix string) {
	log.Debugf("%sEmptyFilter", prefix)
}
