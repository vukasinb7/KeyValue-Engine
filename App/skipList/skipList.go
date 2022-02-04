package skipList
// Header file for SkipList structure
// Contains SkipList and skipListNode structures and constructor for SkipList

type skipListNode struct {
	key string
	value []byte
	next []*skipListNode
}

type SkipList struct {
	head *skipListNode
	size int
	height int
	maxHeight int
}

func NewSkipList(maxHeight int) SkipList {
	next := make([]*skipListNode, maxHeight, maxHeight)
	next[0] = nil
	head := &skipListNode{
		key:   "",
		value: nil,
		next:  next,
	}
	skipList := SkipList{
		head:      head,
		size:      0,
		height:    0,
		maxHeight: maxHeight,
	}
	return skipList
}



