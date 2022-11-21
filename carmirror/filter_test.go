package carmirror

import (
	"testing"

	"github.com/fission-codes/go-bloom"
	"github.com/zeebo/xxh3"
)

func IdHash(id MockBlockId, seed uint64) uint64 {
	return xxh3.HashSeed(id[:], seed)
}

func TestBloomFilter(t *testing.T) {

	bloom := NewBloomFilter[MockBlockId, bloom.HashFunction[MockBlockId]](64, IdHash)

	for i := 0; i < 40; i++ {
		id := RandId()
		err := bloom.Add(id)
		if err != nil {
			t.Errorf("failed to insert item")
		}
		if bloom.DoesNotContain(id) {
			t.Errorf("failed to retrieve item")
		}
	}

}
