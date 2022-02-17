package main

import (
	"SSTable"
	"io/ioutil"
	"log"
	"math"
	"pair"
)

type LSM struct {
	lsmLevels LSMlevel
	maxLvl    uint32
	dirPath   string
}

type LSMlevel struct {
	manager   *SSTable.SSTableManager
	nextLevel *LSMlevel
	size      uint32
	threshold uint32
}

func (lsmLvl *LSMlevel) createSSTable(pairs []pair.KVPair) {
	size, err := lsmLvl.manager.CreateSSTable(pairs)
	if err != nil {
		return
	}
	lsmLvl.size += size

	if lsmLvl.size > lsmLvl.threshold {

	}
}

func newLSM(maxLvl uint32, dirPath string) LSM {
	lsm := LSM{
		maxLvl:  maxLvl,
		dirPath: dirPath,
	}

	levels, err := ioutil.ReadDir(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	baseThreshold := float64(1024)
	tempLevels := make([]LSMlevel, maxLvl, maxLvl)
	for i := 0; i < len(levels); i++ {
		tempLevel := LSMlevel{
			manager:   SSTable.CreateSSTableMng(dirPath + levels[i].Name()),
			size:      0,
			threshold: uint32(baseThreshold * math.Pow(10, float64(i))),
			nextLevel: nil,
		}
		tempLevels[i] = tempLevel
	}

	for i := 0; i < len(levels)-1; i++ {
		tempLevels[i].nextLevel = &tempLevels[i+1]
	}
	lsm.lsmLevels = tempLevels[0]

	return lsm
}

func (lsm *LSM) createLevelTables(pairs []pair.KVPair) {
	lsm.lsmLevels.createSSTable(pairs)
}

func main() {
	newLSM(4, "Data/LSM/")
}
