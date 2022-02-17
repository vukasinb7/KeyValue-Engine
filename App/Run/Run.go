package main

import (
	"SSTable"
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
	ss := SSTable.CreateSSTableMng(configurationManager.DefaultConfiguration.GetSSTableDirectory())
	for _, val := range data {
		err := w.PushRecord(val, true)
		if err != nil {
			log.Fatal(err)
		}
		memtable.Insert(val)
		if memtable.Size() > memtable.Threshold() {
			err := ss.CreateSSTable(memtable.Flush())
			if err != nil {
				return
			}
			w.ResetWAL()
		}

	}
	err := ss.CreateSSTable(memtable.Flush())
	if err != nil {
		return
	}
	w.ResetWAL()

}
