package skipList
// Public methods for SkipList structure

import (
	"errors"
	"fmt"
)

func (skipList *SkipList) Insert(key string, value []byte) error{
	// Insert key value pair into the SkipList
	// Throws error if the key is duplicate

	path := make([]*skipListNode, 0, skipList.height)
	currentNode := skipList.head
	level := skipList.height
	for ;level >= 0;{
		if currentNode.key == key{
			return errors.New("duplicate key")
		}
		if currentNode.next[level] == nil || (currentNode.next[level].key > key && level > 0) {
			path = append(path, currentNode)
			level--
		} else if currentNode.next[level].key <= key{
			currentNode = currentNode.next[level]
		} else{
			path = append(path, currentNode)
			break
		}
	}
	newNodeHeight := skipList.roll()
	newNodeNext := make([]*skipListNode, newNodeHeight + 1, newNodeHeight + 1)
	newNode := skipListNode{
		key:   key,
		value: value,
	}
	for i := skipList.height; i >= 0; i--{
		currentLevel := skipList.height - i
		if currentLevel > newNodeHeight{
			break
		}
		newNodeNext[currentLevel] = path[i].next[currentLevel]
		path[i].next[currentLevel] = &newNode
	}
	if newNodeHeight > skipList.height{
		newNodeNext[newNodeHeight] = nil
		skipList.head.next[newNodeHeight] = &newNode
		skipList.height++
	}
	newNode.next = newNodeNext
	skipList.size++
	return nil
}

func (skipList *SkipList) Get(key string) ([]byte, error){
	// Returns the value of the element with key
	// Throws error if key is not found
	currentNode := skipList.head
	level := skipList.height
	for ;level >= 0;{
		if currentNode.key == key{
			return currentNode.value, nil
		}
		if currentNode.next[level] == nil || (currentNode.next[level].key > key && level > 0) {
			level--
		} else if currentNode.next[level].key <= key{
			currentNode = currentNode.next[level]
		} else{
			break
		}
	}
	return []byte{}, errors.New("The key is not in the list")
}

func (skipList *SkipList) Size() int{
	return skipList.size
}

func (skipList *SkipList) Height() int{
	return skipList.height
}

func (skipList *SkipList) MaxHeight() int{
	return skipList.maxHeight
}

func (skipList *SkipList) Print(){
	fmt.Printf("SkipList {size: %d, height: %d, maxHeight: %d}",skipList.size, skipList.height, skipList.maxHeight)
}