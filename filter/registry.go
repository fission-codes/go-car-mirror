package filter

import (
	"sync"

	"github.com/fission-codes/go-bloom"
	"github.com/zeebo/xxh3"
)

// Registry is a global registry of hash functions.
type Registry struct {
	functions map[uint64]any
	lock      sync.RWMutex
}

var registry Registry = Registry{
	functions: make(map[uint64]any),
	lock:      sync.RWMutex{},
}

// RegistryLookup looks up a hash function by its ID.
func RegistryLookup[T any](id uint64) (bloom.HashFunction[T], bool) {
	registry.lock.RLock()
	defer registry.lock.RUnlock()
	if untyped, ok := registry.functions[id]; ok {
		if typed, ok := untyped.(bloom.HashFunction[T]); ok {
			return typed, true
		}
	}
	return nil, false
}

// RegisterHash registers a hash function with the global registry.
func RegisterHash[T any](id uint64, hash bloom.HashFunction[T]) {
	registry.lock.Lock()
	defer registry.lock.Unlock()
	registry.functions[id] = hash
}

const XXH3_HASH_32_BYTES = 1

// XXH3Hash32Bytes is a 32-bit hash function for byte arrays.
func XXH3Hash32Bytes(id [32]byte, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}

func init() {
	RegisterHash(XXH3_HASH_32_BYTES, XXH3Hash32Bytes)
}
