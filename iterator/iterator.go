package iterator

import "errors"

var Done = errors.New("no more items in iterator")

type Iterator[T any] interface {
	// TODO: Fill in
	Next() (T, error)
}
