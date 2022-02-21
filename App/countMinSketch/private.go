package countMinSketch

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/rand"
)

func calculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func calculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func createHashFunctions(k uint) ([]hash.Hash32, []uint32) {
	var h []hash.Hash32
	var seeds []uint32
	for i := uint(0); i < k; i++ {
		seed := rand.Uint32()
		seeds = append(seeds, seed)
		h = append(h, murmur3.New32WithSeed(seed))
	}
	return h, seeds
}

func hashFunctionsFromSeeds(seeds []uint32) []hash.Hash32 {
	var h []hash.Hash32
	for i := 0; i < len(seeds); i++ {
		h = append(h, murmur3.New32WithSeed(seeds[i]))
	}
	return h
}

func (cms *CountMinSketch) getIndex(byteArray []byte, hfunc hash.Hash32) uint {
	_, err := hfunc.Write(byteArray)
	if err != nil {
		panic(err)
	}
	hashVal := hfunc.Sum32()
	hfunc.Reset()
	hashReduced := uint(hashVal) % cms.m
	return hashReduced
}
