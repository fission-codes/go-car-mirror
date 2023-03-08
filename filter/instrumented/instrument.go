package instrumented

import (
	"encoding/json"
	"fmt"

	filter "github.com/fission-codes/go-car-mirror/filter"
	stats "github.com/fission-codes/go-car-mirror/stats"
	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"
)

type Filter[K comparable] struct {
	filter filter.Filter[K]
	stats  stats.Stats
}

// NewFilter returns a new Filter.
func New[K comparable](filter filter.Filter[K], stats stats.Stats) *Filter[K] {
	return &Filter[K]{filter, stats}
}

// DoesNotContain returns true if the item is not in the filter.
func (rf *Filter[K]) DoesNotContain(item K) bool {
	result := rf.filter.DoesNotContain(item)
	rf.stats.Log(fmt.Sprintf("DoesNotContain.%v", result))
	return result
}

// TryAddAll adds all items from the other filter to the filter.
func (rf *Filter[K]) TryAddAll(other filter.Filter[K]) error {
	err := rf.filter.TryAddAll(other)
	if err == nil {
		rf.stats.Log("TryAddAll.Ok")
	} else {
		rf.stats.Log(fmt.Sprintf("TryAddAll.%v", err))
	}
	return err
}

// AddAll adds all items from the other filter to the filter.
func (rf *Filter[K]) AddAll(other filter.Filter[K]) filter.Filter[K] {
	rf.filter = rf.filter.AddAll(other)
	rf.stats.Log("AddAll")
	return rf
}

// Add adds the item to the filter.
func (rf *Filter[K]) Add(item K) filter.Filter[K] {
	rf.filter = rf.filter.Add(item)
	rf.stats.Log("Add")
	return rf
}

// Clear clears the filter.
func (rf *Filter[K]) Clear() filter.Filter[K] {
	rf.stats.Log("Clear")
	return &Filter[K]{filter: rf.filter.Clear()}
}

// UnsynchronizedCopy returns an unsynchronized copy of the filter.
// func (rf *Filter[K]) UnsynchronizedCopy() filter.Filter[K] {
// 	rf.stats.Log("UnsynchronizedCopy")
//  TODO: This is a recursive loop.  Also, can't use rf.filter.UnsynchronizedCopy() because it is not part of the interface.
// 	return &Filter[K]{rf.UnsynchronizedCopy(), rf.stats}
// }

// Copy returns a copy of the filter.
func (rf *Filter[K]) Copy() filter.Filter[K] {
	rf.stats.Log("Copy")
	return &Filter[K]{rf.filter.Copy(), rf.stats}
}

// Capacity returns the capacity of the filter.
func (rf *Filter[K]) Capacity() int {
	rf.stats.Log("Capacity")
	return rf.filter.Capacity()
}

// Count returns the number of items in the filter.
func (rf *Filter[K]) Count() int {
	rf.stats.Log("Count")
	return rf.filter.Count()
}

// Equal returns true if the other filter is equal to the filter.
func (sf *Filter[K]) Equal(other filter.Filter[K]) bool {
	sf.stats.Log("Equal")
	osf, ok := other.(*Filter[K])
	if !ok {
		return false
	}
	return sf.filter.Equal(osf.filter)
}

// UnmarshalJSON unmarshals the JSON-encoded wire format into the Filter.
func (sf *Filter[K]) UnmarshalJSON(bytes []byte) error {
	sf.stats.LogBytes("UnmarshalJson", uint64(len(bytes)))
	wireFormat := filter.FilterWireFormat[K]{}
	err := json.Unmarshal(bytes, &wireFormat)
	if err == nil {
		sf.filter = wireFormat.Any()
		sf.stats.Log("UnmarshalJSON.Ok")
	} else {
		sf.stats.Log(fmt.Sprintf("UnmarshalJSON.%v", err))
	}
	return err
}

// UnmarshalCBOR unmarshals the CBOR-encoded wire format into the Filter.
func (sf *Filter[K]) UnmarshalCBOR(bytes []byte) error {
	sf.stats.LogBytes("UnmarshalCBOR", uint64(len(bytes)))
	wireFormat := filter.FilterWireFormat[K]{}
	err := cbor.Unmarshal(bytes, &wireFormat)
	if err == nil {
		sf.filter = wireFormat.Any()
		sf.stats.Log("UnmarshalCBOR.Ok")
	} else {
		sf.stats.Log(fmt.Sprintf("UnmarshalCBOR.%v", err))
	}
	return err
}

// MarshalJSON marshals the Filter into the JSON-encoded wire format.
func (sf *Filter[K]) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(filter.NewFilterWireFormat(sf.filter))
	if err == nil {
		sf.stats.Log("MarshalJSON.Ok")
	} else {
		sf.stats.Log(fmt.Sprintf("MarshalJSON.%v", err))
	}
	sf.stats.LogBytes("MarshalJSON", uint64(len(bytes)))
	return bytes, err
}

// MarshalCBOR marshals the Filter into the CBOR-encoded wire format.
func (sf *Filter[K]) MarshalCBOR() ([]byte, error) {
	bytes, err := cbor.Marshal(filter.NewFilterWireFormat(sf.filter))
	if err == nil {
		sf.stats.Log("MarshalCBOR.Ok")
	} else {
		sf.stats.Log(fmt.Sprintf("MarshalCBOR.%v", err))
	}
	sf.stats.LogBytes("MarshalCBOR", uint64(len(bytes)))
	return bytes, err
}

// Dump dumps the filter to the log.
func (sf *Filter[K]) Dump(log *zap.SugaredLogger, prefix string) {
	log.Debugf("%sinstrumented.Filter {", prefix)
	sf.filter.Dump(log, prefix+"  ")
	log.Debugf("%s}", prefix)
}
