package util

import (
	"errors"
	"sync"
)

var ErrListEmpty = errors.New("list is empty")
var ErrListFull = errors.New("list is full")

type Deque[T any] interface {
	// Last element or nil pointer
	Back() *T
	// First element or nil pointer
	Front() *T
	Len() int
	PushBack(v T) error
	PushFront(v T) error
	PopBack() (T, error)
	PopFront() (T, error)
	Capacity() int // -1 for unbounded
}

type ArrayDeque[T any] struct {
	data  []T
	front int
	back  int
}

func NewArrayDeque[T any](capacity int) *ArrayDeque[T] {
	return &ArrayDeque[T]{
		make([]T, capacity),
		capacity - 1,
		0,
	}
}

func (dq *ArrayDeque[T]) Capacity() int {
	return len(dq.data)
}

func (dq *ArrayDeque[T]) dec(index int) int {
	index--
	if index < 0 {
		index += len(dq.data)
	}
	return index
}

func (dq *ArrayDeque[T]) inc(index int) int {
	index++
	if index == len(dq.data) {
		index = 0
	}
	return index
}

func (dq *ArrayDeque[T]) Back() *T {
	return &dq.data[dq.dec(dq.back)]
}

func (dq *ArrayDeque[T]) Front() *T {
	return &dq.data[dq.inc(dq.front)]
}

func (dq *ArrayDeque[T]) Len() int {
	if dq.front > dq.back {
		return len(dq.data) - dq.front + dq.back - 1
	} else {
		return dq.back - dq.front - 1
	}
}

func (dq *ArrayDeque[T]) PushBack(v T) error {
	capacity := len(dq.data)
	if dq.Len() == capacity {
		return ErrListFull
	} else {
		dq.data[dq.back] = v
		dq.back = dq.inc(dq.back)
		return nil
	}
}

func (dq *ArrayDeque[T]) PopBack() (T, error) {
	if dq.Len() == 0 {
		return *new(T), ErrListEmpty
	} else {
		dq.back = dq.dec(dq.back)
		return dq.data[dq.back], nil
	}
}

func (dq *ArrayDeque[T]) PushFront(v T) error {
	capacity := len(dq.data)
	if dq.Len() == capacity {
		return ErrListFull
	} else {
		dq.data[dq.front] = v
		dq.front = dq.dec(dq.front)
		return nil
	}
}

func (dq *ArrayDeque[T]) PopFront() (T, error) {
	if dq.Len() == 0 {
		return *new(T), ErrListEmpty
	} else {
		dq.front = dq.inc(dq.front)
		return dq.data[dq.front], nil
	}
}

type SynchronizedDeque[T any] struct {
	deque   Deque[T]
	mutex   *sync.RWMutex
	condvar *sync.Cond
}

func NewSynchronizedDeque[T any](deque Deque[T]) *SynchronizedDeque[T] {
	mutex := new(sync.RWMutex)
	return &SynchronizedDeque[T]{
		deque,
		mutex,
		sync.NewCond(mutex),
	}
}

func (dq *SynchronizedDeque[T]) Capacity() int {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Capacity()
}

func (dq *SynchronizedDeque[T]) Back() *T {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Back()
}

func (dq *SynchronizedDeque[T]) Front() *T {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Front()
}

func (dq *SynchronizedDeque[T]) Len() int {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Len()
}

func (dq *SynchronizedDeque[T]) PushBack(v T) error {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	result := dq.deque.PushBack(v)
	dq.condvar.Signal()
	return result
}

func (dq *SynchronizedDeque[T]) PopBack() (T, error) {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	return dq.deque.PopBack()
}

func (dq *SynchronizedDeque[T]) PollBack() T {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	for dq.deque.Len() == 0 {
		dq.condvar.Wait()
	}
	res, err := dq.deque.PopBack()
	if err != nil { // really shouldn't get an error here
		panic(err)
	}
	return res
}

func (dq *SynchronizedDeque[T]) PushFront(v T) error {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	result := dq.deque.PushFront(v)
	dq.condvar.Signal()
	return result
}

func (dq *SynchronizedDeque[T]) PopFront() (T, error) {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	return dq.deque.PopFront()
}

func (dq *SynchronizedDeque[T]) PollFront() T {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	for dq.deque.Len() == 0 {
		dq.condvar.Wait()
	}
	res, err := dq.deque.PopFront()
	if err != nil { // really shouldn't get an error here
		panic(err)
	}
	return res
}
