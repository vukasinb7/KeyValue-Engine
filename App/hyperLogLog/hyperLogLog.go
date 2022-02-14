package hyperLogLog

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/bits"
)

type HyperLogLog struct {
	p uint			// number of significant leading bits
	m uint 			// number of buckets
	rc float64		// regulating constant
	buckets []uint8
	hashFunction hash.Hash32
}

func NewHyperLogLog(p uint) HyperLogLog{
	m := uint(math.Pow(2, float64(p)))
	var rc float64
	if m <= 16{
		rc = 0.637
	} else if m == 32{
		rc = 0.697
	} else if m == 64{
		rc = 0.709
	} else {
		rc = 0.7213 / (1.0 + 1.079/float64(m))
	}
	hll := HyperLogLog{
		p:            p,
		m:            m,
		buckets:      make([]uint8, m, m),
		hashFunction: murmur3.New32(),
		rc:			  rc,
	}
	return hll
}

func (hll *HyperLogLog) Insert(byteArray []byte){
	_, err := hll.hashFunction.Write(byteArray)
	if err != nil{
		panic(err)
	}
	hashVal := hll.hashFunction.Sum32()
	hll.hashFunction.Reset()
	trailingZeros := bits.TrailingZeros32(hashVal)
	bucketIndex := hashVal >> (32 - hll.p)
	hll.buckets[bucketIndex] = uint8(math.Max(float64(hll.buckets[bucketIndex]), float64(trailingZeros)))
}

func (hll *HyperLogLog) Cardinality() float64{
	var hmean float64 = 0
	for _, val := range hll.buckets{
		hmean += math.Pow(2, -float64(val))
	}
	z := math.Pow(hmean, -1)
	cardinality := hll.rc * math.Pow(float64(hll.m), 2) * z
	return cardinality
}