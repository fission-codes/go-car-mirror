package util

import (
	"container/list"
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
	head  int
	tail  int
	empty bool
}

func NewArrayDeque[T any](capacity int) *ArrayDeque[T] {
	return &ArrayDeque[T]{
		make([]T, capacity),
		0,
		0,
		true,
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
	if dq.empty {
		return nil
	} else {
		return &dq.data[dq.dec(dq.tail)]
	}
}

func (dq *ArrayDeque[T]) Front() *T {
	if dq.empty {
		return nil
	} else {
		return &dq.data[dq.head]
	}
}

func (dq *ArrayDeque[T]) Len() int {

	if dq.head > dq.tail {
		return len(dq.data) - dq.head + dq.tail
	} else if dq.head < dq.tail {
		return dq.tail - dq.head
	} else {
		if dq.empty {
			return 0
		} else {
			return len(dq.data)
		}
	}
}

func (dq *ArrayDeque[T]) PushBack(v T) error {
	if dq.head == dq.tail && !dq.empty {
		return ErrListFull
	} else {
		dq.data[dq.tail] = v
		dq.tail = dq.inc(dq.tail)
		dq.empty = false
		return nil
	}
}

func (dq *ArrayDeque[T]) PopBack() (T, error) {
	if dq.empty {
		return *new(T), ErrListEmpty
	} else {
		dq.tail = dq.dec(dq.tail)
		if dq.head == dq.tail {
			dq.empty = true
		}
		return dq.data[dq.tail], nil
	}
}

func (dq *ArrayDeque[T]) PushFront(v T) error {
	capacity := len(dq.data)
	if dq.Len() == capacity {
		return ErrListFull
	} else {
		dq.head = dq.dec(dq.head)
		dq.data[dq.head] = v
		dq.empty = false
		return nil
	}
}

func (dq *ArrayDeque[T]) PopFront() (T, error) {
	if dq.empty {
		return *new(T), ErrListEmpty
	} else {
		result := dq.data[dq.head]
		dq.head = dq.inc(dq.head)
		if dq.head == dq.tail {
			dq.empty = true
		}
		return result, nil
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

type ListDeque[T any] struct {
	list list.List
}

func NewListDeque[T any]() *ListDeque[T] {
	return &ListDeque[T]{
		list.List{},
	}
}

func (dq *ListDeque[T]) Capacity() int {
	return -1
}

func (dq *ListDeque[T]) Back() *T {
	if element, ok := dq.list.Back().Value.(T); ok {
		return &element
	} else {
		panic("wrong type for list member")
	}
}

func (dq *ListDeque[T]) Front() *T {
	if element, ok := dq.list.Front().Value.(T); ok {
		return &element
	} else {
		panic("wrong type for list member")
	}
}

func (dq *ListDeque[T]) Len() int {
	return dq.list.Len()
}

func (dq *ListDeque[T]) PushBack(v T) error {
	dq.list.PushBack(v)
	return nil
}

func (dq *ListDeque[T]) PopBack() (T, error) {
	back := dq.list.Back()
	if back == nil {
		return *new(T), ErrListEmpty
	}
	dq.list.Remove(back)
	if result, ok := back.Value.(T); ok {
		return result, nil
	} else {
		panic("wrong type for list member")
	}
}

func (dq *ListDeque[T]) PushFront(v T) error {
	dq.list.PushFront(v)
	return nil
}

func (dq *ListDeque[T]) PopFront() (T, error) {
	front := dq.list.Front()
	if front == nil {
		return *new(T), ErrListEmpty
	}
	dq.list.Remove(front)
	if result, ok := front.Value.(T); ok {
		return result, nil
	} else {
		panic("wrong type for list member")
	}
}

type BlocksDeque[T any] struct {
	blocks    ListDeque[*ArrayDeque[T]]
	blocksize int
}

func NewBlocksDeque[T any](blocksize int) *BlocksDeque[T] {
	result := &BlocksDeque[T]{
		*NewListDeque[*ArrayDeque[T]](),
		blocksize,
	}
	result.blocks.PushFront(NewArrayDeque[T](blocksize))
	return result
}

func (dq *BlocksDeque[T]) Capacity() int {
	return -1
}

func (dq *BlocksDeque[T]) frontBlock() *ArrayDeque[T] {
	return *dq.blocks.Front()
}

func (dq *BlocksDeque[T]) backBlock() *ArrayDeque[T] {
	return *dq.blocks.Back()
}

func (dq *BlocksDeque[T]) Back() *T {
	return dq.backBlock().Back()
}

func (dq *BlocksDeque[T]) Front() *T {
	return dq.frontBlock().Front()
}

func (dq *BlocksDeque[T]) Len() int {
	if dq.blocks.Len() == 1 {
		return dq.frontBlock().Len()
	}
	return dq.frontBlock().Len() + dq.backBlock().Len() + dq.blocksize*(dq.blocks.Len()-2)
}

func (dq *BlocksDeque[T]) PushBack(v T) error {
	if err := dq.backBlock().PushBack(v); err == ErrListFull {
		dq.blocks.PushBack(NewArrayDeque[T](dq.blocksize))
		return dq.backBlock().PushBack(v)
	} else {
		return err
	}
}

func (dq *BlocksDeque[T]) PopBack() (T, error) {
	if result, err := dq.backBlock().PopBack(); err != nil {
		return result, err
	} else {
		if dq.backBlock().empty && dq.blocks.Len() > 1 {
			_, err = dq.blocks.PopBack()
		}
		return result, err
	}
}

func (dq *BlocksDeque[T]) PushFront(v T) error {
	if err := dq.frontBlock().PushFront(v); err == ErrListFull {
		dq.blocks.PushFront(NewArrayDeque[T](dq.blocksize))
		return dq.frontBlock().PushFront(v)
	} else {
		return err
	}
}

func (dq *BlocksDeque[T]) PopFront() (T, error) {
	if result, err := dq.frontBlock().PopFront(); err != nil {
		return result, err
	} else {
		if dq.frontBlock().empty && dq.blocks.Len() > 1 {
			_, err = dq.blocks.PopFront()
		}
		return result, err
	}
}
