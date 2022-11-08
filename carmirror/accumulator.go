package carmirror

import "github.com/fission-codes/go-car-mirror/iterator"

type SimpleStatusAccumulator[I BlockId, B Block[I], F BlockIdFilter[I], IT iterator.Iterator[I], H BlockIdHashMap[I, IT]] struct {
	have F
	want H
}
