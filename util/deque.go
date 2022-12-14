package util

import (
	"container/list"
	"errors"
	"sync"
)

// Errors that can be returned by the util package.
var (
	ErrListEmpty = errors.New("list is empty")
	ErrListFull  = errors.New("list is full")
)

// Deque is a double-ended queue.
type Deque[T any] interface {
	// Back returns the last element of the deque or a nil pointer.
	Back() *T
	// Front returns the first element of the deque or a nil pointer.
	Front() *T
	// Len returns the number of elements in the deque.
	Len() int
	// PushBack adds an element to the back of the deque.
	PushBack(v T) error
	// PushFront adds an element to the front of the deque.
	PushFront(v T) error
	// PopBack removes the last element from the deque and returns it.
	PopBack() (T, error)
	// PopFront removes the first element from the deque and returns it.
	PopFront() (T, error)
	// Capacity returns the maximum number of elements that can be stored in the deque.
	Capacity() int // -1 for unbounded
}

// ArrayDeque is a double-ended queue implemented using an array.
type ArrayDeque[T any] struct {
	data  []T
	head  int
	tail  int
	empty bool
}

// NewArrayDeque creates a new ArrayDeque with the given capacity.
func NewArrayDeque[T any](capacity int) *ArrayDeque[T] {
	return &ArrayDeque[T]{
		make([]T, capacity),
		0,
		0,
		true,
	}
}

// Capacity returns the maximum number of elements that can be stored in the deque.
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

// Back returns the last element of the deque or a nil pointer.
func (dq *ArrayDeque[T]) Back() *T {
	if dq.empty {
		return nil
	} else {
		return &dq.data[dq.dec(dq.tail)]
	}
}

// Front returns the first element of the deque or a nil pointer.
func (dq *ArrayDeque[T]) Front() *T {
	if dq.empty {
		return nil
	} else {
		return &dq.data[dq.head]
	}
}

// Len returns the number of elements in the deque.
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

// PushBack adds an element to the back of the deque.
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

// PopBack removes the last element from the deque and returns it.
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

// PushFront adds an element to the front of the deque.
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

// PopFront removes the first element from the deque and returns it.
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

// SynchronizedDeque is a double-ended queue that is thread-safe.
type SynchronizedDeque[T any] struct {
	deque   Deque[T]
	mutex   *sync.RWMutex
	condvar *sync.Cond
}

// NewSynchronizedDeque creates a new SynchronizedDeque.
func NewSynchronizedDeque[T any](deque Deque[T]) *SynchronizedDeque[T] {
	mutex := new(sync.RWMutex)
	return &SynchronizedDeque[T]{
		deque,
		mutex,
		sync.NewCond(mutex),
	}
}

// Capacity returns the maximum number of elements that can be stored in the deque.
func (dq *SynchronizedDeque[T]) Capacity() int {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Capacity()
}

// Back returns the last element of the deque or a nil pointer.
func (dq *SynchronizedDeque[T]) Back() *T {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Back()
}

// Front returns the first element of the deque or a nil pointer.
func (dq *SynchronizedDeque[T]) Front() *T {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Front()
}

// Len returns the number of elements in the deque.
func (dq *SynchronizedDeque[T]) Len() int {
	dq.mutex.RLock()
	defer dq.mutex.RUnlock()
	return dq.deque.Len()
}

// PushBack adds an element to the back of the deque.
func (dq *SynchronizedDeque[T]) PushBack(v T) error {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	result := dq.deque.PushBack(v)
	dq.condvar.Signal()
	return result
}

// PopBack removes the last element from the deque and returns it.
func (dq *SynchronizedDeque[T]) PopBack() (T, error) {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	return dq.deque.PopBack()
}

// PollBack removes the last element from the deque and returns it.
// If the deque is empty, it will block until an element is available.
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

// PushFront adds an element to the front of the deque.
func (dq *SynchronizedDeque[T]) PushFront(v T) error {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	result := dq.deque.PushFront(v)
	dq.condvar.Signal()
	return result
}

// PopFront removes the first element from the deque and returns it.
func (dq *SynchronizedDeque[T]) PopFront() (T, error) {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	return dq.deque.PopFront()
}

// PollFront removes the first element from the deque and returns it.
// If the deque is empty, it will block until an element is available.
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

// ListDeque is a double-ended queue that is implemented using a linked list.
type ListDeque[T any] struct {
	list list.List
}

// NewListDeque creates a new ListDeque.
func NewListDeque[T any]() *ListDeque[T] {
	return &ListDeque[T]{
		list.List{},
	}
}

// Capacity returns the maximum number of elements that can be stored in the deque.
func (dq *ListDeque[T]) Capacity() int {
	return -1
}

// Back returns the last element of the deque or a nil pointer.
func (dq *ListDeque[T]) Back() *T {
	if element, ok := dq.list.Back().Value.(T); ok {
		return &element
	} else {
		panic("wrong type for list member")
	}
}

// Front returns the first element of the deque or a nil pointer.
func (dq *ListDeque[T]) Front() *T {
	if element, ok := dq.list.Front().Value.(T); ok {
		return &element
	} else {
		panic("wrong type for list member")
	}
}

// Len returns the number of elements in the deque.
func (dq *ListDeque[T]) Len() int {
	return dq.list.Len()
}

// PushBack adds an element to the back of the deque.
func (dq *ListDeque[T]) PushBack(v T) error {
	dq.list.PushBack(v)
	return nil
}

// PopBack removes the last element from the deque and returns it.
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

// PushFront adds an element to the front of the deque.
func (dq *ListDeque[T]) PushFront(v T) error {
	dq.list.PushFront(v)
	return nil
}

// PopFront removes the first element from the deque and returns it.
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

// BlocksDeque is a double-ended queue that is implemented using a linked list of blocks.
type BlocksDeque[T any] struct {
	blocks    ListDeque[*ArrayDeque[T]]
	blocksize int
}

// NewBlocksDeque creates a new BlocksDeque.
func NewBlocksDeque[T any](blocksize int) *BlocksDeque[T] {
	result := &BlocksDeque[T]{
		*NewListDeque[*ArrayDeque[T]](),
		blocksize,
	}
	result.blocks.PushFront(NewArrayDeque[T](blocksize))
	return result
}

// Capacity returns the maximum number of elements that can be stored in the deque.
func (dq *BlocksDeque[T]) Capacity() int {
	return -1
}

func (dq *BlocksDeque[T]) frontBlock() *ArrayDeque[T] {
	return *dq.blocks.Front()
}

func (dq *BlocksDeque[T]) backBlock() *ArrayDeque[T] {
	return *dq.blocks.Back()
}

// Back returns the last element of the deque or a nil pointer.
func (dq *BlocksDeque[T]) Back() *T {
	return dq.backBlock().Back()
}

// Front returns the first element of the deque or a nil pointer.
func (dq *BlocksDeque[T]) Front() *T {
	return dq.frontBlock().Front()
}

// Len returns the number of elements in the deque.
func (dq *BlocksDeque[T]) Len() int {
	if dq.blocks.Len() == 1 {
		return dq.frontBlock().Len()
	}
	return dq.frontBlock().Len() + dq.backBlock().Len() + dq.blocksize*(dq.blocks.Len()-2)
}

// PushBack adds an element to the back of the deque.
func (dq *BlocksDeque[T]) PushBack(v T) error {
	if err := dq.backBlock().PushBack(v); err == ErrListFull {
		dq.blocks.PushBack(NewArrayDeque[T](dq.blocksize))
		return dq.backBlock().PushBack(v)
	} else {
		return err
	}
}

// PopBack removes the last element from the deque and returns it.
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

// PushFront adds an element to the front of the deque.
func (dq *BlocksDeque[T]) PushFront(v T) error {
	if err := dq.frontBlock().PushFront(v); err == ErrListFull {
		dq.blocks.PushFront(NewArrayDeque[T](dq.blocksize))
		return dq.frontBlock().PushFront(v)
	} else {
		return err
	}
}

// PopFront removes the first element from the deque and returns it.
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
