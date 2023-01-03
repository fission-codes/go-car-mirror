package carmirror

import (
	"sync"

	"github.com/fission-codes/go-car-mirror/filter"
	"golang.org/x/exp/maps"
)

// SimpleStatusAccumulator is a simple implementation of StatusAccumulator.
// Its operations are protected by a mutex, so it is safe to use from multiple goroutines.
type SimpleStatusAccumulator[I BlockId] struct {
	have   filter.Filter[I]
	want   map[I]bool // map of blocks that are currently wanted
	wanted map[I]bool // map of blocks that have been wanted, to prevent sending duplicate wants
	mutex  sync.Mutex
}

// NewSimpleStatusAccumulator creates a new SimpleStatusAccumulator.
func NewSimpleStatusAccumulator[I BlockId](filter filter.Filter[I]) *SimpleStatusAccumulator[I] {
	return &SimpleStatusAccumulator[I]{
		have:   filter,
		want:   make(map[I]bool),
		wanted: make(map[I]bool),
		mutex:  sync.Mutex{},
	}
}

// Have marks the given block as having been received.
func (ssa *SimpleStatusAccumulator[I]) Have(id I) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	ssa.have = ssa.have.Add(id)
	return nil
}

func (ssa *SimpleStatusAccumulator[I]) HaveCount() uint {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	return uint(ssa.have.Count())
}

// Want marks the given block as wanted.
func (ssa *SimpleStatusAccumulator[I]) Want(id I) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	if _, ok := ssa.wanted[id]; !ok {
		ssa.want[id] = true
	}
	return nil
}

func (ssa *SimpleStatusAccumulator[I]) WantCount() uint {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	return uint(len(ssa.want))
}

// Receive marks the given block as received.
func (ssa *SimpleStatusAccumulator[I]) Receive(id I) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	delete(ssa.want, id)
	delete(ssa.wanted, id)
	return nil
}

// Send sends the current status using the given StatusSender.
func (ssa *SimpleStatusAccumulator[I]) Send(sender StatusSender[I]) error {
	ssa.mutex.Lock()
	sendHave := ssa.have
	sendWant := maps.Keys(ssa.want)
	ssa.have = ssa.have.Clear() // this aways creates an empty copy
	ssa.want = make(map[I]bool)
	for _, k := range sendWant {
		ssa.wanted[k] = true
	}
	ssa.mutex.Unlock()
	// avoid holding the mutex for the duration of the send operation.
	return sender.SendStatus(sendHave, sendWant)
}
