package queue

import (
	"container/heap"
)

type PriorityQueueItem interface {
	Less(other PriorityQueueItem) bool
}

type priorityQueue []PriorityQueueItem

// Len returns the length of the ExpiryQueue.
func (pq priorityQueue) Len() int { return len(pq) }

// Less is used to order ExpiryQueueItem's by their expiry such that items with
// the oldest expiry are less than items with newer expiries.
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Less(pq[j])
}

// Swap swaps two items in the ExpiryQueue. Swap is used by heap.Interface.
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds a new item the the ExpiryQueue.
func (pq *priorityQueue) Push(x interface{}) {
	item := x.(PriorityQueueItem)
	*pq = append(*pq, item)
}

// Pop removes the top item from the ExpiryQueue. The top item is always the
// one that holds the next value to expire.
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

type PriorityQueue struct {
	queue priorityQueue
}

func (pq *PriorityQueue) Len() int {
	return len(pq.queue)
}

func (pq *PriorityQueue) Empty() bool {
	return len(pq.queue) == 0
}

func (pq *PriorityQueue) Push(item PriorityQueueItem) {
	heap.Push(&pq.queue, item)
}

func (pq *PriorityQueue) Pop() PriorityQueueItem {
	return heap.Pop(&pq.queue).(PriorityQueueItem)
}

func (pq *PriorityQueue) Top() PriorityQueueItem {
	return pq.queue[0]
}
