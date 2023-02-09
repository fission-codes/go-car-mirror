package util

import (
	"math/rand"
	"testing"
)

func RandomInsert(queue Deque[int], entries int) {
	front := rand.Intn(entries)
	for i := 0; i < front; i++ {
		queue.PushFront(-1 - i)
	}
	for i := 0; i < entries-front; i++ {
		queue.PushBack(-1 - i)
	}
}

func RandomRemove(queue Deque[int]) {
	for *queue.Front() < 0 {
		queue.PopFront()
	}
	for *queue.Back() < 0 {
		queue.PopBack()
	}
}

func ModelTestLenForward(queue Deque[int], t *testing.T) {
	capacity := queue.Capacity()
	if capacity < 0 {
		capacity = 10
	}
	for i := 0; i < capacity; i++ {
		if queue.Len() != i {
			t.Errorf("Wrong length for queue, expected %v got %v", i, queue.Len())
		}
		queue.PushBack(i)
	}
	if queue.Len() != capacity {
		t.Errorf("Wrong length for queue, expected %v got %v", capacity, queue.Len())
	}
}

func ModelTestLenReverse(queue Deque[int], t *testing.T) {
	capacity := queue.Capacity()
	if capacity < 0 {
		capacity = 10
	}
	for i := 0; i < capacity; i++ {
		if queue.Len() != i {
			t.Errorf("Wrong length for queue, expected %v got %v", i, queue.Len())
		}
		queue.PushFront(i)
	}
	if queue.Len() != capacity {
		t.Errorf("Wrong length for queue, expected %v got %v", capacity, queue.Len())
	}
}

func ModelTestAsForwardQueue(queue Deque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		capacity := queue.Capacity()
		if capacity > 0 && capacity < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", capacity, queue.Len())
		}
		if capacity < 0 {
			capacity = queue.Len() + entries + 10
		}
		padding := rand.Intn(capacity - queue.Len() - entries)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		initLen := queue.Len()
		for i := 0; i < entries; i++ {
			if err := queue.PushBack(i); err != nil {
				t.Errorf("got error %v when pushing", err)
			}
		}
		if queue.Len()-initLen != entries {
			t.Errorf("On iteration %v, Bad length for queue, want %v, got %v", repeat, entries, queue.Len())
		}
		RandomRemove(queue)
		for i := 0; i < entries; i++ {
			if value, err := queue.PopFront(); err != nil || value != i {
				t.Errorf("On iteration %v/%v, Got error or incorrect value %v, %v", repeat, i, value, err)
			}
		}
	}
}

func ModelTestAsForwardQueueExceedCapacity(queue Deque[int], repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		padding := rand.Intn(queue.Capacity() - queue.Len() - 2)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		remaining := queue.Capacity() - queue.Len()
		for i := 0; i < remaining; i++ {
			if err := queue.PushBack(i); err != nil {
				t.Errorf("Error pushing to back of queue %v", err)
			}
		}
		if err := queue.PushBack(remaining); err != ErrListFull {
			t.Errorf("Wrong/NoError exceeding queue capacity %v", err)
		}
		RandomRemove(queue)
		for i := 0; i < remaining; i++ {
			if value, err := queue.PopFront(); err != nil || value != i {
				t.Errorf("On iteration %v/%v, Got error or incorrect value %v, %v", repeat, i, value, err)
			}
		}
	}
}

func ModelTestAsReverseQueueExceedCapacity(queue Deque[int], repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		padding := rand.Intn(queue.Capacity() - queue.Len() - 2)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		remaining := queue.Capacity() - queue.Len()
		for i := 0; i < remaining; i++ {
			if err := queue.PushFront(i); err != nil {
				t.Errorf("Error pushing to back of queue %v", err)
			}
		}
		if err := queue.PushFront(remaining); err != ErrListFull {
			t.Errorf("Wrong/NoError exceeding queue capacity %v", err)
		}
		RandomRemove(queue)
		for i := 0; i < remaining; i++ {
			if value, err := queue.PopBack(); err != nil || value != i {
				t.Errorf("On iteration %v/%v, Got error or incorrect value %v, %v", repeat, i, value, err)
			}
		}
	}
}

func ModelTestAsPollingForwardQueue(queue *SynchronizedDeque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		capacity := queue.Capacity()
		if capacity > 0 && capacity < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}

		go func() {
			for i := 0; i < entries; i++ {
				queue.PushBack(i)
			}
		}()

		for i := 0; i < entries; i++ {
			value := queue.PollFront()
			if value != i {
				t.Errorf("On iteration %v/%v, Got incorrect value %v", repeat, i, value)
			}
		}
	}
}

func ModelTestAsForwardStack(queue Deque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		capacity := queue.Capacity()
		if capacity > 0 && capacity < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := 10
		if capacity > 0 {
			padding = rand.Intn(capacity - queue.Len() - entries)
		}
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		initLen := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushBack(i)
		}
		if queue.Len()-initLen != entries {
			t.Errorf("On iteration %v, Bad length for queue, want %v, got %v", repeat, entries, queue.Len())
		}
		RandomRemove(queue)
		for i := entries - 1; i >= 0; i-- {
			if value, err := queue.PopBack(); err != nil || value != i {
				t.Errorf("On iteration %v/%v, Got error or incorrect value %v, %v", repeat, i, value, err)
			}
		}
	}
}

func ModelTestAsReverseQueue(queue Deque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		capacity := queue.Capacity()
		if capacity > 0 && capacity < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := 10
		if capacity > 0 {
			padding = rand.Intn(capacity - queue.Len() - entries)
		}
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		initLen := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushFront(i)
		}
		if queue.Len()-initLen != entries {
			t.Errorf("On iteration %v, Bad length for queue, want %v, got %v", repeat, entries, queue.Len())
		}
		RandomRemove(queue)
		for i := 0; i < entries; i++ {
			if value, err := queue.PopBack(); err != nil || value != i {
				t.Errorf("On iteration %v/%v, Got error or incorrect value %v, %v", repeat, i, value, err)
			}
		}
	}
}

func ModelTestAsPollingReverseQueue(queue *SynchronizedDeque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		capacity := queue.Capacity()
		if capacity > 0 && capacity < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}

		go func() {
			for i := 0; i < entries; i++ {
				queue.PushFront(i)
			}
		}()

		for i := 0; i < entries; i++ {
			value := queue.PollBack()
			if value != i {
				t.Errorf("On iteration %v/%v, Got incorrect value %v", repeat, i, value)
			}
		}
	}
}

func ModelTestAsReverseStack(queue Deque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		capacity := queue.Capacity()
		if capacity > 0 && capacity < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := 10
		if capacity > 0 {
			padding = rand.Intn(capacity - queue.Len() - entries)
		}
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		initLen := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushFront(i)
		}
		if queue.Len()-initLen != entries {
			t.Errorf("On iteration %v, Bad length for queue, want %v, got %v", repeat, entries, queue.Len())
		}
		RandomRemove(queue)
		for i := entries - 1; i >= 0; i-- {
			if value, err := queue.PopFront(); err != nil || value != i {
				t.Errorf("On iteration %v/%v, Got error or incorrect value %v, %v", repeat, i, value, err)
			}
		}
	}
}

func TestArrayQueueAsForwardQueue(t *testing.T) {
	ModelTestAsForwardQueue(NewArrayDeque[int](50), 20, 20, t)
}

func TestArrayQueueLenForward(t *testing.T) {
	ModelTestLenForward(NewArrayDeque[int](10), t)
}

func TestArrayQueueAsForwardQueueExceedCapacity(t *testing.T) {
	ModelTestAsForwardQueueExceedCapacity(NewArrayDeque[int](50), 20, t)
}

func TestArrayQueueAsReverseQueue(t *testing.T) {
	ModelTestAsReverseQueue(NewArrayDeque[int](50), 20, 20, t)
}

func TestArrayQueueLenReverse(t *testing.T) {
	ModelTestLenReverse(NewArrayDeque[int](10), t)
}

func TestArrayQueueAsBackwardQueueExceedCapacity(t *testing.T) {
	ModelTestAsReverseQueueExceedCapacity(NewArrayDeque[int](50), 20, t)
}

func TestArrayQueueAsForwardStack(t *testing.T) {
	ModelTestAsForwardStack(NewArrayDeque[int](50), 20, 20, t)
}

func TestArrayQueueAsReverseStack(t *testing.T) {
	ModelTestAsReverseStack(NewArrayDeque[int](50), 20, 20, t)
}

func TestListQueueAsForwardQueue(t *testing.T) {
	ModelTestAsForwardQueue(NewListDeque[int](), 20, 20, t)
}

func TestListQueueLenForward(t *testing.T) {
	ModelTestLenForward(NewListDeque[int](), t)
}

func TestListQueueAsReverseQueue(t *testing.T) {
	ModelTestAsReverseQueue(NewListDeque[int](), 20, 20, t)
}

func TestListQueueLenReverse(t *testing.T) {
	ModelTestLenReverse(NewListDeque[int](), t)
}

func TestListQueueAsForwardStack(t *testing.T) {
	ModelTestAsForwardStack(NewListDeque[int](), 20, 20, t)
}

func TestListQueueAsReverseStack(t *testing.T) {
	ModelTestAsReverseStack(NewListDeque[int](), 20, 20, t)
}

func TestBlocksQueueAsForwardQueue(t *testing.T) {
	ModelTestAsForwardQueue(NewBlocksDeque[int](5), 20, 20, t)
}

func TestBlocksQueueLenForward(t *testing.T) {
	ModelTestLenForward(NewBlocksDeque[int](5), t)
}

func TestBlocksQueueAsReverseQueue(t *testing.T) {
	ModelTestAsReverseQueue(NewBlocksDeque[int](5), 20, 20, t)
}

func TestBlocksQueueLenReverse(t *testing.T) {
	ModelTestLenReverse(NewBlocksDeque[int](5), t)
}

func TestBlocksQueueAsForwardStack(t *testing.T) {
	ModelTestAsForwardStack(NewBlocksDeque[int](5), 20, 20, t)
}

func TestBlocksQueueAsReverseStack(t *testing.T) {
	ModelTestAsReverseStack(NewBlocksDeque[int](5), 20, 20, t)
}

func TestRandomly(t *testing.T) {
	queue := NewArrayDeque[int](50)
	for i := 0; i < 100; i++ {
		test := rand.Intn(4)
		switch test {
		case 0:
			ModelTestAsForwardQueue(queue, 20, 0, t)
		case 1:
			ModelTestAsReverseQueue(queue, 20, 0, t)
		case 2:
			ModelTestAsForwardStack(queue, 20, 0, t)
		case 3:
			ModelTestAsReverseStack(queue, 20, 0, t)
		}
	}
}

func TestSynchronizedQueueAsForwardQueue(t *testing.T) {
	ModelTestAsForwardQueue(NewSynchronizedDeque[int](NewArrayDeque[int](50)), 20, 20, t)
}

func TestSynchronizedQueueAsPollingForwardQueue(t *testing.T) {
	ModelTestAsPollingForwardQueue(NewSynchronizedDeque[int](NewArrayDeque[int](50)), 20, 20, t)
}

func TestSynchronizedQueueAsReverseQueue(t *testing.T) {
	ModelTestAsReverseQueue(NewSynchronizedDeque[int](NewArrayDeque[int](50)), 20, 20, t)
}

func TestSynchronizedQueueAsPollingReverseQueue(t *testing.T) {
	ModelTestAsPollingReverseQueue(NewSynchronizedDeque[int](NewArrayDeque[int](50)), 20, 20, t)
}

func TestSynchronizedQueueAsForwardStack(t *testing.T) {
	ModelTestAsForwardStack(NewSynchronizedDeque[int](NewArrayDeque[int](50)), 20, 20, t)
}

func TestSynchronizedQueueAsReverseStack(t *testing.T) {
	ModelTestAsReverseStack(NewSynchronizedDeque[int](NewArrayDeque[int](50)), 20, 20, t)
}
