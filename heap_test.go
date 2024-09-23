package zkafka

import (
	"slices"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func Test_offsetHeap_PushPopPeek_WhenInsertSmaller(t *testing.T) {
	defer recoverThenFail(t)
	heap := offsetHeap{}
	input1 := kafka.TopicPartition{Partition: 1, Offset: 1}
	heap.Push(input1)
	got, err := heap.Peek()
	require.NoError(t, err)
	require.Equal(t, input1, got, "expected the minimum offset, which is the only offset")

	input2 := kafka.TopicPartition{Partition: 1, Offset: 0}
	// insert smaller item into heap
	heap.Push(input2)
	got, err = heap.Peek()
	require.NoError(t, err)
	require.Equal(t, input2, got)

	got = heap.Pop()
	require.Equal(t, input2, got)

	got = heap.Pop()
	require.Equal(t, input1, got)
}

func Test_offsetHeap_PushPopPeek_WhenInsertBigger(t *testing.T) {
	defer recoverThenFail(t)
	heap := offsetHeap{}
	input1 := kafka.TopicPartition{Partition: 1, Offset: 1}
	heap.Push(input1)
	got, err := heap.Peek()
	require.NoError(t, err)
	require.Equal(t, input1, got, "expected the minimum offset, which is the only offset")

	input2 := kafka.TopicPartition{Partition: 1, Offset: 100}
	// insert smaller item into heap
	heap.Push(input2)
	got, err = heap.Peek()
	require.NoError(t, err)
	require.Equal(t, input1, got)

	got = heap.Pop()
	require.Equal(t, input1, got)
}

func Test_offsetHeap_PopWhenEmptyResultsInPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			require.Fail(t, "expected panic on pop when empty")
		}
	}()
	heap := offsetHeap{}
	_ = heap.Pop()
}

func Test_offsetHeap_PeekWhenEmpty(t *testing.T) {
	defer recoverThenFail(t)

	heap := offsetHeap{}
	_, err := heap.Peek()
	require.Error(t, err, "expected error on peek when empty")
}

// Test_offsetHeap_SeekPop_DoesntImpactHeapOrdering
// given 100 items in the heap.
// when n are seek popped (taken out of the middle of the heap)
// then when we heap.Pop we still get the minimum offsets
func Test_offsetHeap_SeekPop_DoesntImpactHeapOrdering(t *testing.T) {
	defer recoverThenFail(t)
	heap := offsetHeap{}
	var offsets []kafka.TopicPartition

	// build up a heap of size N
	count := 100
	for i := 0; i < count; i++ {
		offset := kafka.TopicPartition{Partition: 1, Offset: kafka.Offset(i)}
		offsets = append(offsets, offset)
		heap.Push(offset)
	}
	require.Len(t, heap.data, count)

	// remove M items from heap
	offsetsToRemove := []int{95, 34, 12, 2, 44, 45}
	for _, index := range offsetsToRemove {
		heap.SeekPop(offsets[index])
	}

	remainingCount := count - len(offsetsToRemove)
	require.Len(t, heap.data, remainingCount)

	// Loop through the N items that were in the heap
	// skip the ones known to be seekPopped out
	i := 0
	for i < count {
		if slices.Contains(offsetsToRemove, i) {
			i++
			continue
		}
		got := heap.Pop()
		want := offsets[i]
		require.Equal(t, want, got, "Expect pop to still pop minimums even after seek pops")
		i++
	}
}
