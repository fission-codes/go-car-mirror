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

func ModelTestAsForwardQueue(queue Deque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		if queue.Capacity() < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := rand.Intn(queue.Capacity() - queue.Len() - entries)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		init_len := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushBack(i)
		}
		if queue.Len()-init_len != entries {
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

func ModelTestAsPollingForwardQueue(queue *SynchronizedDeque[int], entries int, repeats int, t *testing.T) {
	for repeat := 0; repeat < repeats; repeat++ {
		if queue.Capacity() < entries {
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
		if queue.Capacity() < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := rand.Intn(queue.Capacity() - queue.Len() - entries)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		init_len := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushBack(i)
		}
		if queue.Len()-init_len != entries {
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
		if queue.Capacity() < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := rand.Intn(queue.Capacity() - queue.Len() - entries)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		init_len := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushFront(i)
		}
		if queue.Len()-init_len != entries {
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
		if queue.Capacity() < entries {
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
		if queue.Capacity() < entries {
			t.Errorf("Queue with capacity %v and len %v has insufficient space for test", queue.Capacity(), queue.Len())
		}
		padding := rand.Intn(queue.Capacity() - queue.Len() - entries)
		if padding > 0 {
			RandomInsert(queue, padding)
		}
		init_len := queue.Len()
		for i := 0; i < entries; i++ {
			queue.PushFront(i)
		}
		if queue.Len()-init_len != entries {
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

func TestArrayQueueAsReverseQueue(t *testing.T) {
	ModelTestAsReverseQueue(NewArrayDeque[int](50), 20, 20, t)
}

func TestArrayQueueAsForwardStack(t *testing.T) {
	ModelTestAsForwardStack(NewArrayDeque[int](50), 20, 20, t)
}

func TestArrayQueueAsReverseStack(t *testing.T) {
	ModelTestAsReverseStack(NewArrayDeque[int](50), 20, 20, t)
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
