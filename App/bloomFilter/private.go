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
	hfunc.Write(byteArray)
	hash := hfunc.Sum32()
	hfunc.Reset()
	hashReduced := uint(hash) % bf.m
	return hashReduced
}

func createHashFunctions(k uint) []hash.Hash32 {
	var h []hash.Hash32
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(rand.Uint32()))
	}
	return h
}