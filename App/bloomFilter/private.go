package bloomFilter

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/rand"
)

func calculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func calculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (bf *BloomFilter) getIndex(byteArray []byte, hfunc hash.Hash32) uint{
	_, err := hfunc.Write(byteArray)
	if err != nil{
		panic(err)
	}
	hashVal := hfunc.Sum32()
	hfunc.Reset()
	hashReduced := uint(hashVal) % bf.m
	return hashReduced
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
	for i := 0; i < len(seeds); i++{
		h = append(h, murmur3.New32WithSeed(seeds[i]))
	}
	return h
}