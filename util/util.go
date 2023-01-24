package util

import (
	"sync"
	"sync/atomic"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

// NextPowerOfTwo returns i if it is a power of 2, otherwise the next power of two greater than i.
func NextPowerOfTwo(i uint64) uint64 {
	i--
	i |= i >> 1
	i |= i >> 2
	i |= i >> 4
	i |= i >> 8
	i |= i >> 16
	i++
	return i
}

// SharedFlagSet is a set of flags that can be shared between goroutines.
type SharedFlagSet[F constraints.Unsigned] struct {
	flags   atomic.Value
	condvar *sync.Cond
}

// NewSharedFlagSet creates a new SharedFlagSet with the given initial value.
func NewSharedFlagSet[F constraints.Unsigned](init F) *SharedFlagSet[F] {
	res := new(SharedFlagSet[F])
	res.flags.Store(init)
	res.condvar = sync.NewCond(new(sync.Mutex))
	return res
}

// All returns the current value of the flag set.
func (fs *SharedFlagSet[F]) All() F {
	return fs.flags.Load().(F)
}

// Update updates the flag set with the given flags.
// The mask flags are cleared, and replaced with the set values.
func (fs *SharedFlagSet[F]) Update(mask F, set F) {
	fs.condvar.L.Lock()
	fs.flags.Store(fs.flags.Load().(F)&^mask | set)
	fs.condvar.L.Unlock()
	fs.condvar.Broadcast()
}

// Set sets the given flags.
func (fs *SharedFlagSet[F]) Set(flags F) {
	fs.Update(0, flags)
}

// Unset clears the given flags.
func (fs *SharedFlagSet[F]) Unset(flags F) {
	fs.Update(flags, 0)
}

// Wait for all the flags in 'state' to be set
func (fs *SharedFlagSet[F]) Wait(state F) {
	fs.condvar.L.Lock()
	for fs.flags.Load().(F)&state != state {
		fs.condvar.Wait()
	}
	fs.condvar.L.Unlock()
}

// Wait for any change on the masked bits from the current value
func (fs *SharedFlagSet[F]) WaitAny(mask F, current F) F {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	for fs.flags.Load().(F)&mask == current {
		fs.condvar.Wait()
	}
	return fs.flags.Load().(F)
}

// WaitExact waits for the masked bits to become exactly current value.
func (fs *SharedFlagSet[F]) WaitExact(mask F, current F) F {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	for fs.flags.Load().(F)&mask != current {
		fs.condvar.Wait()
	}
	return fs.flags.Load().(F)
}

// Contains returns true if all the given flags are set.
func (fs *SharedFlagSet[F]) Contains(flags F) bool {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	return fs.flags.Load().(F)&flags == flags
}

// ContainsAny returns true if any of the given flags are set.
func (fs *SharedFlagSet[F]) ContainsAny(flags F) bool {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	return fs.flags.Load().(F)&flags != 0
}

// ContainsExact returns true if the masked bits have exactly the given values.
func (fs *SharedFlagSet[F]) ContainsExact(mask F, flags F) bool {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	return fs.flags.Load().(F)&mask == flags
}

// Min returns the minimum of the two values.
// This is a convenience function for the constraints.Ordered interface.
func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// Max returns the maximum of the two values.
// This is a convenience function for the constraints.Ordered interface.
func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func Diff[T constraints.Integer](a, b T) T {
	if a > b {
		return a - b
	}
	return b - a
}

// Map applies the given mapper function to each element of the given slice.
func Map[T any, U any](ts []T, mapper func(T) U) []U {
	var result = make([]U, len(ts))
	for i, v := range ts {
		result[i] = mapper(v)
	}
	return result
}

type SynchronizedMap[K comparable, V any] struct {
	data  map[K]V
	mutex sync.RWMutex
}

func NewSynchronizedMap[K comparable, V any]() *SynchronizedMap[K, V] {
	return &SynchronizedMap[K, V]{
		make(map[K]V),
		sync.RWMutex{},
	}
}

func (m *SynchronizedMap[K, V]) Add(key K, value V) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.data[key] = value
}

func (m *SynchronizedMap[K, V]) Remove(key K) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.data, key)
}

func (m *SynchronizedMap[K, V]) Get(key K) (V, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	datum, ok := m.data[key]
	return datum, ok
}

func (m *SynchronizedMap[K, V]) GetOrInsert(key K, creator func() V) V {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	datum, ok := m.data[key]
	if !ok {
		datum = creator()
		m.data[key] = datum
	}
	return datum
}

func (m *SynchronizedMap[K, V]) Keys() []K {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return maps.Keys(m.data)
}
