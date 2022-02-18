package main

import (
	"LSMTree"
	"configurationManager"
	"log"
	"memTable"
	"os"
	"wal"
)

func main() {
	_, errCreate := os.Create("AAA.txt")
	if errCreate != nil {
		log.Fatal(errCreate)
	}
	configurationManager.LoadDefaultConfiguration("Data/defaultConfiguration.json")
	w := wal.CreateWal(configurationManager.DefaultConfiguration.WalSegmentSize, configurationManager.DefaultConfiguration.WalDirectory, configurationManager.DefaultConfiguration.LowWaterMark)
	memtable := memTable.NewMemTable(configurationManager.DefaultConfiguration.MemTableThreshold, configurationManager.DefaultConfiguration.MemTableCapacity)
	data := configurationManager.ParseData(configurationManager.DefaultConfiguration.DataFile)
	lsm := LSMTree.NewLSM(4, configurationManager.DefaultConfiguration.GetLSMDirectory())
	for _, val := range data {
		err := w.PushRecord(val)
		if err != nil {
			log.Fatal(err)
		}
		memtable.Insert(val)
		if memtable.Size() > memtable.Threshold() {
			lsm.CreateLevelTables(memtable.Flush())
			w.ResetWAL()
		}

	}

}
