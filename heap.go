package zkafka

import (
	"container/heap"
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type offsetHeap struct {
	data _offsetHeap
}

// Push adds an offset to the heap
func (h *offsetHeap) Push(offset kafka.TopicPartition) {
	heap.Push(&h.data, offset)
}

// Pop returns the minimum offset from the heap and rearranges the heap to put the new minimum at the root
func (h *offsetHeap) Pop() kafka.TopicPartition {
	if len(h.data) == 0 {
		panic("popped empty heap")
	}
	//nolint:errcheck // access control guarantees type is TopicPartition
	return heap.Pop(&h.data).(kafka.TopicPartition)
}

// Peek returns the minimum offset from the heap without any side effects.
func (h *offsetHeap) Peek() (kafka.TopicPartition, error) {
	if len(h.data) == 0 {
		return kafka.TopicPartition{}, errors.New("peeked empty heap")
	}
	return (h.data)[0], nil
}

// SeekPop linearly searches the heap looking for a match, and removes and returns it.
// If nothing is found, nil is returned and the heap isn't mutated.
// It is an O(n) and therefore is not as efficient as Peek or Pop, but is necessary
// for removing arbitrary items from the data structure
func (h *offsetHeap) SeekPop(partition kafka.TopicPartition) *kafka.TopicPartition {
	for i, d := range h.data {
		if d == partition {
			h.data = append(h.data[:i], h.data[i+1:]...)
			return &d
		}
	}
	return nil
}

// An _offsetHeap is a min-heap of topicPartitions where offset is used to determine order
type _offsetHeap []kafka.TopicPartition

var _ heap.Interface = (*_offsetHeap)(nil)

func (h _offsetHeap) Len() int           { return len(h) }
func (h _offsetHeap) Less(i, j int) bool { return h[i].Offset < h[j].Offset }
func (h _offsetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *_offsetHeap) Push(x any) {
	//nolint:errcheck // access control guarantees type is TopicPartition
	*h = append(*h, x.(kafka.TopicPartition))
}

func (h *_offsetHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
