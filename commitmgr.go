package zkafka

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// topicCommitMgr manages each subscribed topics commit manager.
type topicCommitMgr struct {
	mtx              *sync.RWMutex
	topicToCommitMgr map[string]*commitMgr
}

func newTopicCommitMgr() *topicCommitMgr {
	return &topicCommitMgr{
		mtx:              &sync.RWMutex{},
		topicToCommitMgr: map[string]*commitMgr{}}
}

// get returns a topics commit manager in a thread safe way
func (t *topicCommitMgr) get(topicName string) *commitMgr {
	t.mtx.RLock()
	c, found := t.topicToCommitMgr[topicName]
	t.mtx.RUnlock()
	if !found {
		t.mtx.Lock()
		defer t.mtx.Unlock()
		c, found = t.topicToCommitMgr[topicName]
		if found {
			return c
		}
		c = newCommitMgr()
		t.topicToCommitMgr[topicName] = c
	}
	return c
}

// commitMgr manages inWork and completed commits. Its main responsibility is determining, through TryPop, the largest completed
// offset that can be safely committed (no outstanding inWork offsets with lower offset numbers). It manages these collections of offsets
// on a per-partition basis.
type commitMgr struct {
	// mtx synchronizes access to per partition maps (partitionToMtx, partitionToInWork...)
	mtx *sync.RWMutex
	// partitionToMtx is a map of per partition managed mutexes. Used to synchronize mutations to offsetHeaps
	partitionToMtx map[int32]*sync.Mutex
	// partitionToInWork uses partition index as a key and has a heap as a value. It's responsible for allowing quick determination
	// of the smallest inwork offset
	partitionToInWork map[int32]*offsetHeap
	// partitionToCompleted uses partition index as a key and has a heap as a value. It's responsible for allowing quick determination
	// of the smallest completed offset
	partitionToCompleted map[int32]*offsetHeap
	// inWorkCount is a count of inflight work. Incremented when work is pushed to heap. Decremented when work is completed
	inWorkCount int64
}

func newCommitMgr() *commitMgr {
	return &commitMgr{
		mtx:                  &sync.RWMutex{},
		partitionToMtx:       map[int32]*sync.Mutex{},
		partitionToInWork:    map[int32]*offsetHeap{},
		partitionToCompleted: map[int32]*offsetHeap{},
	}
}

// PushInWork pushes an offset to one of the managed inwork heaps.
func (c *commitMgr) PushInWork(tp kafka.TopicPartition) {
	m := c.mutex(tp.Partition)
	m.Lock()
	heap := c.getInWorkHeap(tp.Partition)
	heap.Push(tp)
	atomic.AddInt64(&c.inWorkCount, 1)
	m.Unlock()
}

// RemoveInWork removes an arbitrary partition from the heap (not necessarily the minimum).
// If the pop is successful (the partition is found) the in work count is decremented
func (c *commitMgr) RemoveInWork(tp kafka.TopicPartition) {
	m := c.mutex(tp.Partition)
	m.Lock()
	heap := c.getInWorkHeap(tp.Partition)
	if heap.SeekPop(tp) != nil {
		atomic.AddInt64(&c.inWorkCount, -1)
	}
	m.Unlock()
}

// PushCompleted pushes an offset to one of the managed completed heaps
func (c *commitMgr) PushCompleted(tp kafka.TopicPartition) {
	m := c.mutex(tp.Partition)
	m.Lock()
	heap := c.getCompletedHeap(tp.Partition)
	heap.Push(tp)
	atomic.AddInt64(&c.inWorkCount, -1)
	m.Unlock()
}

func (c *commitMgr) InWorkCount() int64 {
	return atomic.LoadInt64(&c.inWorkCount)
}

// TryPop returns the largest shared offset between inWork and completed. If
// none such offset exists nil is returned.
// TryPop is thread safe
func (c *commitMgr) TryPop(_ context.Context, partition int32) *kafka.TopicPartition {
	m := c.mutex(partition)
	m.Lock()
	defer m.Unlock()
	var commitOffset *kafka.TopicPartition
	for {
		inWorkPeek, err := c.peekInWork(partition)
		if err != nil {
			break
		}
		completedPeek, err := c.peekCompleted(partition)
		if err != nil {
			break
		}

		if completedPeek.Offset == inWorkPeek.Offset {
			_ = c.popCompleted(partition)
			_ = c.popInWork(partition)
			commitOffset = &completedPeek
		} else {
			break
		}
	}
	return commitOffset
}

// mutex returns a per partition mutex used for managing offset heaps at a per partition granularity
func (c *commitMgr) mutex(partition int32) *sync.Mutex {
	c.mtx.RLock()
	mtx, found := c.partitionToMtx[partition]
	c.mtx.RUnlock()
	if !found {
		c.mtx.Lock()
		defer c.mtx.Unlock()
		mtx, found = c.partitionToMtx[partition]
		if found {
			return mtx
		}
		mtx = &sync.Mutex{}
		c.partitionToMtx[partition] = mtx
	}
	return mtx
}

// getInWorkHeap returns the in work offsetHeap for a particular partition in a thread safe way
func (c *commitMgr) getInWorkHeap(partition int32) *offsetHeap {
	c.mtx.RLock()
	h, found := c.partitionToInWork[partition]
	c.mtx.RUnlock()
	if !found {
		c.mtx.Lock()
		defer c.mtx.Unlock()
		h, found = c.partitionToInWork[partition]
		if found {
			return h
		}
		h = &offsetHeap{}
		c.partitionToInWork[partition] = h
	}
	return h
}

// getCompletedHeap returns the completed offsetHeap for a particular partition in a thread safe way
func (c *commitMgr) getCompletedHeap(partition int32) *offsetHeap {
	c.mtx.RLock()
	h, found := c.partitionToCompleted[partition]
	c.mtx.RUnlock()
	if !found {
		c.mtx.Lock()
		defer c.mtx.Unlock()
		h, found = c.partitionToCompleted[partition]
		if found {
			return h
		}
		h = &offsetHeap{}
		c.partitionToCompleted[partition] = h
	}
	return h
}

func (c *commitMgr) popInWork(partition int32) kafka.TopicPartition {
	heap := c.getInWorkHeap(partition)
	out := heap.Pop()
	return out
}

func (c *commitMgr) popCompleted(partition int32) kafka.TopicPartition {
	heap := c.getCompletedHeap(partition)
	out := heap.Pop()
	return out
}

func (c *commitMgr) peekInWork(partition int32) (kafka.TopicPartition, error) {
	heap := c.getInWorkHeap(partition)
	return heap.Peek()
}
func (c *commitMgr) peekCompleted(partition int32) (kafka.TopicPartition, error) {
	heap := c.getCompletedHeap(partition)
	return heap.Peek()
}
