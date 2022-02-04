package bloomFilter

import (
	"fmt"
)

func (bf *BloomFilter) Print(){
	fmt.Printf("BloomFilter {Size of bit-array: %d, False positive rate: %f, Number of hash functions: %d}\n", bf.m, bf.p, bf.k)
}

func (bf *BloomFilter) Insert(byteArray []byte){
	for _, hfunc := range bf.hashFunctions{
		index := bf.getIndex(byteArray, hfunc)
		bf.arr[index] = 1
	}
}

func (bf *BloomFilter) Contains(byteArray []byte) bool{

	for _, hfunc := range bf.hashFunctions{
		index := bf.getIndex(byteArray, hfunc)
		val := bf.arr[index]
		if val != 1 {return false}
	}
	return true
}