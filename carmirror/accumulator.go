package carmirror

import (
	"sync"

	. "github.com/fission-codes/go-car-mirror/filter"
	"golang.org/x/exp/maps"
)

type SimpleStatusAccumulator[I BlockId] struct {
	have  Filter[I]
	want  map[I]bool
	mutex sync.Mutex
}

func NewSimpleStatusAccumulator[I BlockId](filter Filter[I]) *SimpleStatusAccumulator[I] {
	return &SimpleStatusAccumulator[I]{
		have:  filter,
		want:  make(map[I]bool),
		mutex: sync.Mutex{},
	}
}

func (ssa *SimpleStatusAccumulator[I]) Have(id I) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	ssa.have = ssa.have.Add(id)
	return nil
}

func (ssa *SimpleStatusAccumulator[I]) Want(id I) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	ssa.want[id] = true
	return nil
}

func (ssa *SimpleStatusAccumulator[I]) Receive(id I) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	delete(ssa.want, id)
	return nil
}

func (ssa *SimpleStatusAccumulator[I]) Send(sender StatusSender[I]) error {
	ssa.mutex.Lock()
	defer ssa.mutex.Unlock()
	sender.SendStatus(ssa.have, maps.Keys(ssa.want))
	ssa.have = ssa.have.Clear()
	return nil
}
