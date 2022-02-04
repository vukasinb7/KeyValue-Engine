package bloomFilter

import (
	"hash"
)

type BloomFilter struct{
	m uint			// size of bit-array
	p float64		// false positive rate
	k uint			// number of hash functions
	arr []byte		// byte array
	hashFunctions []hash.Hash32		// array of hash functions
}
func NewBloomFilter(p float64, n int) BloomFilter{
	m := calculateM(n, p)
	k := calculateK(n, m)
	bf := BloomFilter{
		m:             m,
		p:             p,
		k:             k,
		arr:           make([]byte, m, m),
		hashFunctions: createHashFunctions(k),
	}
	return bf
}

