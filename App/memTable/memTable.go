package memTable

import (
	"math"
	"skipList"
)

type MemTable struct {
	threshold uint32            // size of MemTable in bytes after which flush operation should be called
	capacity  uint32            // maximum size of MemTable in bytes
	size      uint32            // current size of MemTable in bytes
	list      skipList.SkipList // data storage
}

func NewMemTable(threshold, capacity uint32) MemTable {
	skipListHeight := int(math.Log2(float64(capacity)))
	memTable := MemTable{
		threshold: threshold,
		capacity:  capacity,
		size:      0,
		list:      skipList.NewSkipList(skipListHeight),
	}
	return memTable
}
