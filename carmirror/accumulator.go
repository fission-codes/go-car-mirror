package carmirror

import "github.com/fission-codes/go-car-mirror/iterator"

type SimpleStatusAccumulator[I BlockId, ITI iterator.Iterator[I], B Block[I], F Filter[I, F], IT iterator.Iterator[I]] struct {
	have F
	want map[I]bool
}
