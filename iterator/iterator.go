package iterator

import "errors"

var ErrDone = errors.New("no more items in iterator")

type Iterator[T any] interface {
	Next() (*T, error)
}

type SliceIterator[T any] struct {
	items []T
	idx   int
}

func NewSliceIterator[T any](items []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		items: items,
		idx:   0,
	}
}

func (i SliceIterator[T]) Next() (*T, error) {
	if i.idx < len(i.items) {
		i.idx += 1
		return &i.items[i.idx-1], nil
	} else {
		return nil, ErrDone
	}
}
