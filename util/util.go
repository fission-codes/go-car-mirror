package util

import (
	"sync"
	"sync/atomic"

	"golang.org/x/exp/constraints"
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

type SharedFlagSet[F constraints.Unsigned] struct {
	flags   atomic.Value
	condvar *sync.Cond
}

func NewSharedFlagSet[F constraints.Unsigned](init F) *SharedFlagSet[F] {
	res := new(SharedFlagSet[F])
	res.flags.Store(init)
	res.condvar = sync.NewCond(new(sync.Mutex))
	return res
}

func (fs *SharedFlagSet[F]) GetAll() F {
	return fs.flags.Load().(F)
}

func (fs *SharedFlagSet[F]) Update(unset F, set F) {
	fs.condvar.L.Lock()
	fs.flags.Store(fs.flags.Load().(F)&^unset | set)
	fs.condvar.L.Unlock()
	fs.condvar.Broadcast()
}

func (fs *SharedFlagSet[F]) Set(flags F) {
	fs.Update(0, flags)
}

func (fs *SharedFlagSet[F]) Unset(flags F) {
	fs.Update(flags, 0)
}

func (fs *SharedFlagSet[F]) Wait(status F) {
	fs.condvar.L.Lock()
	for fs.flags.Load().(F)&status != status {
		fs.condvar.Wait()
	}
	fs.condvar.L.Unlock()
}

func (fs *SharedFlagSet[F]) WaitAny(mask F, current F) F {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	for fs.flags.Load().(F)&mask == current {
		fs.condvar.Wait()
	}
	return fs.flags.Load().(F)
}

func (fs *SharedFlagSet[F]) WaitExact(mask F, current F) F {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	for fs.flags.Load().(F)&mask != current {
		fs.condvar.Wait()
	}
	return fs.flags.Load().(F)
}

func (fs *SharedFlagSet[F]) Contains(flags F) bool {
	fs.condvar.L.Lock()
	defer fs.condvar.L.Unlock()
	return fs.flags.Load().(F)&flags == flags
}
