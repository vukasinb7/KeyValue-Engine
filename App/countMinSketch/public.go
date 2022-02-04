package countMinSketch

import (
	"fmt"
	"math"
)

func (cms *CountMinSketch) Print(){
	fmt.Printf("CountMinSketch {Table rows count: %d, Table columns count: %d, Precision: %f, Accuracy: %f}\n", cms.k, cms.m, cms.prs, cms.acc)
}

func (cms *CountMinSketch) Insert(byteArray []byte){
	for rowIndex, hfunc := range cms.hashFunctions{
		columnIndex := cms.getIndex(byteArray, hfunc)
		index := uint(rowIndex) * cms.m + columnIndex
		cms.arr[index]++
	}
}

func (cms *CountMinSketch) Count(byteArray []byte) uint{
	currentMin := uint(math.MaxUint)
	for rowIndex, hfunc := range cms.hashFunctions{
		columnIndex := cms.getIndex(byteArray, hfunc)
		index := uint(rowIndex) * cms.m + columnIndex
		val := cms.arr[index]
		if val < currentMin{
			currentMin = val
		}
	}
	return currentMin
}