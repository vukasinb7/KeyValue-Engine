package bloomFilter

import (
	"encoding/binary"
	"fmt"
	"math"
)

func (bf *BloomFilter) Print(){
	fmt.Printf("BloomFilter {Size of bit-array: %d, False positive rate: %f, Number of hash functions: %d}\n", bf.m, bf.p, bf.k)
}

func (bf *BloomFilter) Insert(byteArray []byte){
	for _, hfunc := range bf.hashFunctions {
		index := bf.getIndex(byteArray, hfunc)
		bf.arr[index] = 1
	}
}

func (bf *BloomFilter) Contains(byteArray []byte) bool{

	for _, hfunc := range bf.hashFunctions {
		index := bf.getIndex(byteArray, hfunc)
		val := bf.arr[index]
		if val != 1 {return false}
	}
	return true
}

func (bf *BloomFilter) Encode() []byte{
	arrLen := uint32(len(bf.arr))
	hLen := uint32(len(bf.seeds))
	SIZE := 24 + arrLen + hLen * 4
	output := make([]byte, SIZE, SIZE)
	binary.LittleEndian.PutUint32(output[:], uint32(bf.m))
	binary.LittleEndian.PutUint64(output[4:], math.Float64bits(bf.p))
	binary.LittleEndian.PutUint32(output[12:], uint32(bf.k))
	binary.LittleEndian.PutUint32(output[16:], arrLen)
	for i := 0; i < int(arrLen); i++{
		output[20 + i] = bf.arr[i]
	}
	binary.LittleEndian.PutUint32(output[20 + arrLen:], hLen)
	for i := uint32(0); i < hLen; i++{
		index := 24 + arrLen + i * 4
		binary.LittleEndian.PutUint32(output[index:], bf.seeds[i])
	}
	return output
}

func Decode(bytes []byte) BloomFilter{
	m := uint(binary.LittleEndian.Uint32(bytes[:]))
	p := math.Float64frombits(binary.LittleEndian.Uint64(bytes[4:]))
	k := uint(binary.LittleEndian.Uint32(bytes[12:]))
	arrLen := int(binary.LittleEndian.Uint32(bytes[16:]))
	arr := make([]byte, arrLen, arrLen)
	for i := 0; i < arrLen; i++{
		arr[i] = bytes[20 + i]
	}
	hLen := int(binary.LittleEndian.Uint32(bytes[20 + arrLen:]))
	seeds := make([]uint32, hLen, hLen)
	for i := 0; i < hLen; i++{
		index := 24 + arrLen + i * 4
		seeds[i] = binary.LittleEndian.Uint32(bytes[index:])
	}
	bf := BloomFilter{
		m:             m,
		p:             p,
		k:             k,
		arr:           arr,
		hashFunctions: hashFunctionsFromSeeds(seeds),
		seeds:         seeds,
	}
	return bf
}

