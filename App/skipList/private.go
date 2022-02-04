package skipList
// Private and helper methods for SkipList structure

import "math/rand"

func (skipList *SkipList) roll() int {
	level := 0

	for ; rand.Int31n(2) == 1; level++ {
		if level > skipList.height {
			return level
		}
	}
	return level
}