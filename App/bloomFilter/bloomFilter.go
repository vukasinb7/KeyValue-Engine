package bloomFilter

import (
	"hash"
)

type BloomFilter struct{
	m             uint          // size of bit-array
	p             float64       // false positive rate
	k             uint          // number of hash functions
	arr           []byte        // byte array
	hashFunctions []hash.Hash32 // array of hash functions
	seeds		  []uint32
}
func NewBloomFilter(p float64, n int) BloomFilter{
	m := calculateM(n, p)
	k := calculateK(n, m)
	bf := BloomFilter{
		m:             m,
		p:             p,
		k:             k,
		arr:           make([]byte, m, m),
		seeds:		   make([]uint32, 0),
	}
	hashFunctions, seeds := createHashFunctions(k)
	bf.hashFunctions = hashFunctions
	bf.seeds = seeds
	return bf
}

