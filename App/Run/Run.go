package main

import (
	"configMng"
	"fmt"
	"log"
	"memTable"
	"os"
	"wal"
)

func main() {
	_, errCreate := os.Create("AA.txt")
	if errCreate != nil {
		log.Fatal(errCreate)
	}
	configurationManager.LoadDefaultConfiguration("Data/defaultConfiguration.json")
	w := wal.CreateWal(configurationManager.DefaultConfiguration.WalSegmentSize, configurationManager.DefaultConfiguration.WalDirectory, configurationManager.DefaultConfiguration.LowWaterMark)
	memtable := memTable.NewMemTable(configurationManager.DefaultConfiguration.MemTableThreshold, configurationManager.DefaultConfiguration.MemTableCapacity)
	data := configurationManager.ParseData(configurationManager.DefaultConfiguration.DataFile)

	for _, val := range data {
		err := w.PushRecord(val, true)
		if err != nil {
			log.Fatal(err)
		}
		memtable.Insert(val)
		if memtable.Size() > memtable.Threshold() {
			fmt.Println(memtable.Flush())
			w.ResetWAL()
		}

	}

}
