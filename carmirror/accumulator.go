package carmirror

import "github.com/fission-codes/go-car-mirror/iterator"

type SimpleStatusAccumulator[I BlockId, ITI iterator.Iterator[I], B Block[I, ITI], F Filter[I], IT iterator.Iterator[I]] struct {
	have F
	want map[I]bool
}
