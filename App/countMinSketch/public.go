package countMinSketch

import (
	"encoding/binary"
	"fmt"
	"math"
)

func (cms *CountMinSketch) Print() {
	fmt.Printf("CountMinSketch {Table rows count: %d, Table columns count: %d, Precision: %f, Accuracy: %f}\n", cms.k, cms.m, cms.prs, cms.acc)
}

func (cms *CountMinSketch) Insert(byteArray []byte) {
	for rowIndex, hfunc := range cms.hashFunctions {
		columnIndex := cms.getIndex(byteArray, hfunc)
		index := uint(rowIndex)*cms.m + columnIndex
		cms.arr[index]++
	}
}

func (cms *CountMinSketch) Count(byteArray []byte) uint {
	currentMin := uint(math.MaxUint)
	for rowIndex, hfunc := range cms.hashFunctions {
		columnIndex := cms.getIndex(byteArray, hfunc)
		index := uint(rowIndex)*cms.m + columnIndex
		val := cms.arr[index]
		if val < currentMin {
			currentMin = val
		}
	}
	return currentMin
}

func (cms *CountMinSketch) Encode() []byte {
	arrLen := uint32(len(cms.arr))
	hLen := uint32(len(cms.seeds))
	SIZE := 32 + arrLen*4 + hLen*4
	output := make([]byte, SIZE, SIZE)
	binary.LittleEndian.PutUint32(output[:], uint32(cms.k))
	binary.LittleEndian.PutUint32(output[4:], uint32(cms.m))
	binary.LittleEndian.PutUint64(output[8:], math.Float64bits(cms.prs))
	binary.LittleEndian.PutUint64(output[16:], math.Float64bits(cms.acc))
	binary.LittleEndian.PutUint32(output[24:], arrLen)
	for i := 0; i < int(arrLen); i++ {
		binary.LittleEndian.PutUint32(output[28+i*4:], uint32(cms.arr[i]))
	}
	binary.LittleEndian.PutUint32(output[28+arrLen*4:], hLen)
	for i := uint32(0); i < hLen; i++ {
		binary.LittleEndian.PutUint32(output[32+arrLen*4+i*4:], cms.seeds[i])
	}
	return output
}

func Decode(bytes []byte) CountMinSketch {
	k := uint(binary.LittleEndian.Uint32(bytes[:]))
	m := uint(binary.LittleEndian.Uint32(bytes[4:]))
	prs := math.Float64frombits(binary.LittleEndian.Uint64(bytes[8:]))
	acc := math.Float64frombits(binary.LittleEndian.Uint64(bytes[16:]))
	arrLen := int(binary.LittleEndian.Uint32(bytes[24:]))
	arr := make([]uint, arrLen, arrLen)
	for i := 0; i < arrLen; i++ {
		arr[i] = uint(binary.LittleEndian.Uint32(bytes[28+i*4:]))
	}
	hLen := int(binary.LittleEndian.Uint32(bytes[28+arrLen*4:]))
	seeds := make([]uint32, hLen, hLen)
	for i := 0; i < hLen; i++ {
		index := 32 + arrLen*4 + i*4
		seeds[i] = binary.LittleEndian.Uint32(bytes[index:])
	}
	cms := CountMinSketch{
		k:             k,
		m:             m,
		prs:           prs,
		acc:           acc,
		arr:           arr,
		seeds:         seeds,
		hashFunctions: hashFunctionsFromSeeds(seeds),
	}
	return cms
}
