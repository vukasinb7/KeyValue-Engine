package hyperLogLog

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"math/bits"
	"math/rand"
)

type HyperLogLog struct {
	p            uint    // number of significant leading bits
	m            uint    // number of buckets
	rc           float64 // regulating constant
	buckets      []uint8
	seed         uint32
	hashFunction hash.Hash32
}

func NewHyperLogLog(p uint) HyperLogLog {
	m := uint(math.Pow(2, float64(p)))
	var rc float64
	if m <= 16 {
		rc = 0.637
	} else if m == 32 {
		rc = 0.697
	} else if m == 64 {
		rc = 0.709
	} else {
		rc = 0.7213 / (1.0 + 1.079/float64(m))
	}

	seed := rand.Uint32()
	hll := HyperLogLog{
		p:            p,
		m:            m,
		buckets:      make([]uint8, m, m),
		hashFunction: murmur3.New32WithSeed(seed),
		seed:         seed,
		rc:           rc,
	}
	return hll
}

func (hll *HyperLogLog) Insert(byteArray []byte) {
	_, err := hll.hashFunction.Write(byteArray)
	if err != nil {
		panic(err)
	}
	hashVal := hll.hashFunction.Sum32()
	hll.hashFunction.Reset()
	trailingZeros := bits.TrailingZeros32(hashVal)
	bucketIndex := hashVal >> (32 - hll.p)
	hll.buckets[bucketIndex] = uint8(math.Max(float64(hll.buckets[bucketIndex]), float64(trailingZeros)))
}

func (hll *HyperLogLog) Cardinality() float64 {
	var hmean float64 = 0
	for _, val := range hll.buckets {
		hmean += math.Pow(2, -float64(val))
	}
	z := math.Pow(hmean, -1)
	cardinality := hll.rc * math.Pow(float64(hll.m), 2) * z
	return cardinality
}

func (hll *HyperLogLog) Encode() []byte {
	bucketsLen := uint32(len(hll.buckets))
	SIZE := 24 + bucketsLen
	output := make([]byte, SIZE, SIZE)
	binary.LittleEndian.PutUint32(output[:], uint32(hll.p))
	binary.LittleEndian.PutUint32(output[4:], uint32(hll.m))
	binary.LittleEndian.PutUint64(output[8:], math.Float64bits(hll.rc))
	binary.LittleEndian.PutUint32(output[16:], hll.seed)
	binary.LittleEndian.PutUint32(output[20:], bucketsLen)
	for i := 0; i < int(bucketsLen); i++ {
		output[24+i] = hll.buckets[i]
	}

	return output
}

func Decode(bytes []byte) HyperLogLog {
	p := uint(binary.LittleEndian.Uint32(bytes[:]))
	m := uint(binary.LittleEndian.Uint32(bytes[4:]))
	rc := math.Float64frombits(binary.LittleEndian.Uint64(bytes[8:]))
	seed := binary.LittleEndian.Uint32(bytes[16:])
	bucketsLen := int(binary.LittleEndian.Uint32(bytes[20:]))

	buckets := make([]uint8, bucketsLen, bucketsLen)
	for i := 0; i < bucketsLen; i++ {
		buckets[i] = bytes[24+i]
	}

	hll := HyperLogLog{
		p:            p,
		m:            m,
		rc:           rc,
		buckets:      buckets,
		seed:         seed,
		hashFunction: murmur3.New32WithSeed(seed),
	}
	return hll
}
