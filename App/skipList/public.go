package skipList

// Public methods for SkipList structure

import (
	"errors"
	"fmt"
	"pair"
)

func (skipList *SkipList) Insert(kvPair pair.KVPair) bool {
	// ================
	// Description:
	// ================
	// 		Inserts key value pair into the SkipList
	//		Returns status code: false for changing value for existing pair, true for inserting new pair

	path := make([]*skipListNode, 0, skipList.height)
	currentNode := skipList.head
	level := skipList.height
	for level >= 0 {
		if currentNode.key == kvPair.Key {
			currentNode.value = kvPair.Value
			return false
		}
		if currentNode.next[level] == nil || (currentNode.next[level].key > kvPair.Key && level > 0) {
			path = append(path, currentNode)
			level--
		} else if currentNode.next[level].key <= kvPair.Key {
			currentNode = currentNode.next[level]
		} else {
			path = append(path, currentNode)
			break
		}
	}
	newNodeHeight := skipList.roll()
	newNodeNext := make([]*skipListNode, newNodeHeight+1, newNodeHeight+1)
	newNode := skipListNode{
		key:       kvPair.Key,
		value:     kvPair.Value,
		tombstone: 0,
	}
	for i := skipList.height; i >= 0; i-- {
		currentLevel := skipList.height - i
		if currentLevel > newNodeHeight {
			break
		}
		newNodeNext[currentLevel] = path[i].next[currentLevel]
		path[i].next[currentLevel] = &newNode
	}
	if newNodeHeight > skipList.height {
		newNodeNext[newNodeHeight] = nil
		skipList.head.next[newNodeHeight] = &newNode
		skipList.height++
	}
	newNode.next = newNodeNext
	skipList.size++
	return true
}

func (skipList *SkipList) Get(key string) ([]byte, byte, error) {
	// ================
	// Description:
	// ================
	// 		Returns the value of the element with key
	// 		Throws error if key is not found

	currentNode := skipList.head
	level := skipList.height
	for level >= 0 {
		if currentNode.key == key {
			return currentNode.value, currentNode.tombstone, nil
		}
		if currentNode.next[level] == nil || (currentNode.next[level].key > key && level > 0) {
			level--
		} else if currentNode.next[level].key <= key {
			currentNode = currentNode.next[level]
		} else {
			break
		}
	}
	return []byte{}, 0, errors.New("the key is not in the list")
}

func (skipList *SkipList) Delete(key string) bool {
	// ================
	// Description:
	// ================
	// 		Delete key value pair into the SkipList
	//		Returns status code: false for changing tombstone value for existing pair, true for deleting new pair

	path := make([]*skipListNode, 0, skipList.height)
	currentNode := skipList.head
	level := skipList.height
	for level >= 0 {
		if currentNode.key == key {
			currentNode.tombstone = 1
			return false
		}
		if currentNode.next[level] == nil || (currentNode.next[level].key > key && level > 0) {
			path = append(path, currentNode)
			level--
		} else if currentNode.next[level].key <= key {
			currentNode = currentNode.next[level]
		} else {
			path = append(path, currentNode)
			break
		}
	}
	newNodeHeight := skipList.roll()
	newNodeNext := make([]*skipListNode, newNodeHeight+1, newNodeHeight+1)
	newNode := skipListNode{
		key:       key,
		value:     nil,
		tombstone: 1,
	}
	for i := skipList.height; i >= 0; i-- {
		currentLevel := skipList.height - i
		if currentLevel > newNodeHeight {
			break
		}
		newNodeNext[currentLevel] = path[i].next[currentLevel]
		path[i].next[currentLevel] = &newNode
	}
	if newNodeHeight > skipList.height {
		newNodeNext[newNodeHeight] = nil
		skipList.head.next[newNodeHeight] = &newNode
		skipList.height++
	}
	newNode.next = newNodeNext
	skipList.size++
	return true
}

//DELETE
/*func (skipList *SkipList) Delete(key string) ([]byte, error) {
	// ================
	// Description:
	// ================
	// 		Deletes the element with given key
	// 		Returns the value of the element if it is found, else returns error

	path := make([]*skipListNode, 0, skipList.height)
	currentNode := skipList.head
	level := skipList.height
	var output []byte
	for level >= 0 {
		if currentNode.next[level] != nil && currentNode.next[level].key == key {
			path = append(path, currentNode)
			level--
		} else if currentNode.next[level] == nil || (currentNode.next[level].key > key && level > 0) {
			level--
		} else if currentNode.next[level].key < key {
			currentNode = currentNode.next[level]
		} else {
			return nil, errors.New("the key is not in the list")
		}
	}
	output = path[0].next[len(path)-1].value
	for i := len(path) - 1; i >= 0; i-- {
		currentLevel := len(path) - i - 1
		path[i].next[currentLevel] = path[i].next[currentLevel].next[currentLevel]
	}
	if skipList.head.next[skipList.height] == nil {
		skipList.height--
	}
	return output, nil
}*/

func (skipList *SkipList) Size() int {
	return skipList.size
}

func (skipList *SkipList) Height() int {
	return skipList.height
}

func (skipList *SkipList) MaxHeight() int {
	return skipList.maxHeight
}

func (skipList *SkipList) Print() {
	fmt.Printf("SkipList {size: %d, height: %d, maxHeight: %d}\n", skipList.size, skipList.height, skipList.maxHeight)
}

func (skipList *SkipList) GetData() []pair.KVPair {
	// ================
	// Description:
	// ================
	// 		Returns list of key-value pairs

	data := make([]pair.KVPair, 0, skipList.size)
	skipList.ResetIterator()
	key, value, err := skipList.Next()
	for ; err == nil; key, value, err = skipList.Next() {
		data = append(data, pair.KVPair{
			Key:   key,
			Value: value,
		})
	}
	return data

}

func (skipList *SkipList) Next() (string, []byte, error) {
	// ================
	// Description:
	// ================
	// 		Increments iterator
	// 		Returns key and value of element iterator is pointing to
	// 		If the iterator is at the end of data returns error
	//		Use ResetIterator() to iterate through list again
	//
	// ================
	// Example of use:
	// ================
	// 		key, val, err := skipList.Next()
	// 		for ; err == nil; key, val, err = skipList.Next(){
	//			fmt.Println("{",key,",",val,"}")
	// 		}

	nextIter := skipList.iterator.next[0]
	if nextIter == nil {
		return "", nil, errors.New("iterator is at the end of data")
	}

	skipList.iterator = nextIter
	return skipList.iterator.key, skipList.iterator.value, nil
}

func (skipList *SkipList) ResetIterator() {
	// ================
	// Description:
	// ================
	// 		Returns iterator to a beginning of data

	skipList.iterator = skipList.head
}
