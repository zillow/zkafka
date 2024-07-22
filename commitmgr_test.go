package zkafka

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func Test_commitMgr_GetCommitOffset_ShouldReturnProperCandidateCommitOffsetsAndUpdateInternalDataStructs(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	offset1 := kafka.TopicPartition{
		Partition: 1,
		Offset:    1,
	}
	offset2 := kafka.TopicPartition{
		Partition: 1,
		Offset:    2,
	}
	offset3 := kafka.TopicPartition{
		Partition: 1,
		Offset:    3,
	}
	mgr.PushInWork(offset3)
	mgr.PushInWork(offset2)
	mgr.PushInWork(offset1)
	mgr.PushCompleted(offset3)
	got := mgr.TryPop(context.Background(), 1)
	require.Nil(t, got, "expected nil since there are smaller inwork messages than the lowest completed msg.")

	mgr.PushCompleted(offset2)
	got = mgr.TryPop(context.Background(), 1)
	require.Nil(t, got, "expected nil since there are smaller inwork messages than the lowest completed msg.")

	mgr.PushCompleted(offset1)
	got = mgr.TryPop(context.Background(), 1)
	require.NotNil(t, got, "expected commitOffset result to be the largest shared offset between inwork and completed.")
	require.Equal(t, offset3, *got, "expected commitOffset result to be the largest shared offset between inwork and completed.")

	require.Len(t, mgr.partitionToCompleted, 1, "expect only 1 map entry for the single partition touched")
	require.Len(t, mgr.partitionToInWork, 1, "expect only 1 map entry for the single partition touched")

	require.Empty(t, mgr.partitionToCompleted[1].data, "expected last get to empty heap")
	require.Empty(t, mgr.partitionToInWork[1].data, "expected last get to empty heap")
	require.Empty(t, mgr.inWorkCount, "expected inWorkCount to be empty")
}

func Test_commitMgr_GetCommitOffset_ShouldReturnProperCandidateCommitOffsetsAndUpdateInternalDataStructs_WithLargeHeaps(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	var offsets []kafka.TopicPartition
	maxCount := 10000

	var partition int32 = 1
	for i := 0; i < maxCount; i++ {
		offsets = append(offsets, kafka.TopicPartition{
			Partition: partition,
			Offset:    kafka.Offset(i),
		})
		mgr.PushInWork(offsets[i])
	}
	for i := maxCount - 1; i > 0; i-- {
		offsets = append(offsets, kafka.TopicPartition{
			Partition: partition,
			Offset:    kafka.Offset(i),
		})
		mgr.PushCompleted(offsets[i])
		got := mgr.TryPop(context.Background(), partition)
		require.Nil(t, got, "expected nil commit since offset=0 has yet to be committed")
	}

	mgr.PushCompleted(kafka.TopicPartition{Partition: partition, Offset: 0})
	expectedLast := kafka.TopicPartition{Partition: partition, Offset: kafka.Offset(maxCount - 1)}

	got := mgr.TryPop(context.Background(), partition)
	require.NotNil(t, got, "expected commit message")
	require.Equal(t, expectedLast, *got, "expected commit message")

	require.Len(t, mgr.partitionToCompleted, 1, "expect only 1 map entry for the single partition touched")
	require.Len(t, mgr.partitionToInWork, 1, "expect only 1 map entry for the single partition touched")

	require.Empty(t, mgr.partitionToCompleted[1].data, "expected last get to empty heap")
	require.Empty(t, mgr.partitionToInWork[1].data, "expected last get to empty heap")
	require.Empty(t, mgr.inWorkCount, "expected inWorkCount to be empty")
}

func Test_commitMgr_GetCommitOffset_ShouldHandleMultiplePartitions(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	offset1_1 := kafka.TopicPartition{
		Partition: 1,
		Offset:    1,
	}
	offset3_1 := kafka.TopicPartition{
		Partition: 3,
		Offset:    1,
	}
	offset1_2 := kafka.TopicPartition{
		Partition: 1,
		Offset:    2,
	}
	mgr.PushInWork(offset1_1)
	mgr.PushInWork(offset3_1)
	mgr.PushInWork(offset1_2)
	mgr.PushCompleted(offset1_2)
	got := mgr.TryPop(context.Background(), 1)
	require.Nil(t, got, "expected nil since there are smaller inwork messages than the lowest completed msg for this partition")

	mgr.PushCompleted(offset3_1)
	got = mgr.TryPop(context.Background(), 3)
	require.NotNil(t, got)
	require.Equal(t, offset3_1, *got)

	mgr.PushCompleted(offset1_1)
	got = mgr.TryPop(context.Background(), 1)
	require.Equal(t, offset1_2, *got, "expected commitOffset result to be the largest shared offset between inwork and completed")

	require.Len(t, mgr.partitionToCompleted, 2, "expect only 2 map entry for the single partition touched")
	require.Len(t, mgr.partitionToInWork, 2, "expect only 2 map entry for the single partition touched")

	require.Len(t, mgr.partitionToInWork, 2, "expect only 2 map entry for the single partition touched")
	require.Empty(t, mgr.partitionToCompleted[1].data, 2, "expected last get to empty heap")
	require.Empty(t, mgr.partitionToInWork[1].data, 2, "expected last get to empty heap")
	require.Empty(t, mgr.partitionToCompleted[3].data, 2, "expected last get to empty heap")
	require.Empty(t, mgr.partitionToInWork[3].data, 2, "expected last get to empty heap")
	require.Equal(t, int64(0), mgr.inWorkCount, "expected inWorkCount to be empty")
}

// Test_commitMgr_RemoveInWork_CanBeUsedToUpdateInWorkInCommitManager
// RemoveInWork is used in clean up situations. This test shows that offsets that
// are added as inwork can be removed because they are "completed" (typical usage)
// or can be "removed" explicitly (`RemoveInWork` call).
//
// Assertions are made to the inworkcount as well as by using `TryPop` which should only return offsets
// that have had predecessor contiguously committed.
func Test_commitMgr_RemoveInWork_CanBeUsedToUpdateInWorkInCommitManager(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	offset1_1 := kafka.TopicPartition{
		Partition: 1,
		Offset:    1,
	}
	offset1_2 := kafka.TopicPartition{
		Partition: 1,
		Offset:    2,
	}
	offset1_3 := kafka.TopicPartition{
		Partition: 1,
		Offset:    3,
	}
	mgr.PushInWork(offset1_1)
	mgr.PushInWork(offset1_2)

	require.Equal(t, mgr.InWorkCount(), int64(2))

	got := mgr.TryPop(context.Background(), 1)
	require.Nil(t, got, "expected nil since offsets are in work but not completed")

	mgr.PushCompleted(offset1_1)
	got = mgr.TryPop(context.Background(), 1)
	require.NotNil(t, got)
	require.Equal(t, offset1_1, *got)
	require.Equal(t, mgr.InWorkCount(), int64(1))

	mgr.RemoveInWork(offset1_3)
	require.Equal(t, mgr.InWorkCount(), int64(1), "We attempted to remove an offset that wasn't in work. This shouldn't happen, but we show that its non impacting on data structure")

	mgr.RemoveInWork(offset1_2)
	require.Equal(t, mgr.InWorkCount(), int64(0), "Expect in work count to drop since offset was added as 'inwork' and then removed")

	got = mgr.TryPop(context.Background(), 1)
	require.Nil(t, got, "Expected nil since all work has been completed or removed")
}

func Test_commitMgr_PerPartitionDataStructuresBuiltUpConcurrentlyCorrectly(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()

	wg := sync.WaitGroup{}
	partitionCount := 1000
	for i := 0; i < partitionCount; i++ {
		wg.Add(1)
		go func(partition int) {
			mgr.PushInWork(kafka.TopicPartition{Partition: int32(partition)})
			mgr.PushCompleted(kafka.TopicPartition{Partition: int32(partition)})
			wg.Done()
		}(i)
	}
	wg.Wait()
	require.Len(t, mgr.partitionToMtx, partitionCount, "expected partitionToMtx to be have one per visited partition")
	require.Len(t, mgr.partitionToInWork, partitionCount, "expected partitionToInWork to be have one per visited partition")
	require.Len(t, mgr.partitionToCompleted, partitionCount, "expected partitionToCompleted to be have one per visited partition")
	require.Equal(t, int64(0), mgr.inWorkCount, "expected inWorkCount to be empty")
}

// Test_commitMgr_mutex_ShouldReturnReferenceToSameMutexForSamePartition tests for a race condition (based on bugfound)
// where two goroutines calling this method received distinct mutexes (which isn't correct for syncronization purposes).
// Try a large amount of times to access the mutex for a particular partition. Always should return same pointer
func Test_commitMgr_mutex_ShouldReturnReferenceToSameMutexForSamePartition(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	mgr.mutex(0)
	for i := 0; i < 10000; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		var m1 *sync.Mutex
		var m2 *sync.Mutex
		go func() {
			m1 = mgr.mutex(int32(i))
			wg.Done()
		}()
		go func() {
			m2 = mgr.mutex(int32(i))
			wg.Done()
		}()
		wg.Wait()
		require.Same(t, m1, m2, "mutex pointers should be shared for same partition")
	}
}

func Test_commitMgr_mutex_ShouldReturnReferenceToSameCompletedHeapForSamePartition(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	mgr.mutex(0)
	for i := 0; i < 10000; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		var h1 *offsetHeap
		var h2 *offsetHeap
		go func() {
			h1 = mgr.getCompletedHeap(int32(i))
			wg.Done()
		}()
		go func() {
			h2 = mgr.getCompletedHeap(int32(i))
			wg.Done()
		}()
		wg.Wait()
		require.Same(t, h1, h2, "mutex pointers should be shared for same partition")
	}
}

func Test_commitMgr_mutex_ShouldReturnReferenceToSameInWorkHeapForSamePartition(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newCommitMgr()
	mgr.mutex(0)
	for i := 0; i < 10000; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		var h1 *offsetHeap
		var h2 *offsetHeap
		go func() {
			h1 = mgr.getInWorkHeap(int32(i))
			wg.Done()
		}()
		go func() {
			h2 = mgr.getInWorkHeap(int32(i))
			wg.Done()
		}()
		wg.Wait()
		require.Same(t, h1, h2, "mutex pointers should be shared for same partition")
	}
}

func Test_topicCommitMgr_mutex_ShouldReturnReferenceToSameCommitMgrForSameTopic(t *testing.T) {
	defer recoverThenFail(t)
	mgr := newTopicCommitMgr()
	loopCount := 10000
	for i := 0; i < loopCount; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		var h1 *commitMgr
		var h2 *commitMgr
		go func() {
			h1 = mgr.get(strconv.Itoa(i))
			wg.Done()
		}()
		go func() {
			h2 = mgr.get(strconv.Itoa(i))
			wg.Done()
		}()
		wg.Wait()
		require.Same(t, h1, h2, "mutex pointers should be shared for same partition")
	}
	require.Len(t, mgr.topicToCommitMgr, loopCount, "expected managed topics under topicCommitManager to equal number of loops")
}
