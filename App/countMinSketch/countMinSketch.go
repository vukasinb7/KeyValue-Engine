package countMinSketch

import "hash"

type CountMinSketch struct {
	k uint			// number of hash functions or number of table rows
	m uint			// number of columns in table
	prs float64		// precision (epsilon)
	acc float64		// accuracy (delta)
	arr []uint		// table
	hashFunctions []hash.Hash32		// array of hash functions
}

func NewCountMinSketch(prs float64, acc float64) CountMinSketch{
	k := calculateK(acc)
	m := calculateM(prs)
	cms := CountMinSketch{
		k:             k,
		m:             m,
		prs:           prs,
		acc:           acc,
		arr:           make([]uint, k*m, k*m),
		hashFunctions: createHashFunctions(k),
	}
	return cms
}

