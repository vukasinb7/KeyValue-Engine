package main

import (
	"LSMTree"
	"bloomFilter"
	"configurationManager"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"lru"
	"memTable"
	"os"
	"recordUtil"
	"strings"
	"wal"
)

func insertTestData() {
	data := configurationManager.ParseData(configurationManager.DefaultConfiguration.DataFile)
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

var memtable memTable.MemTable
var w wal.Wal
var lsm LSMTree.LSM
var lruCache lru.LRUCache

func main() {
	configurationManager.LoadDefaultConfiguration("Data/defaultConfiguration.json")
	w = wal.CreateWal(configurationManager.DefaultConfiguration.WalSegmentSize, configurationManager.DefaultConfiguration.WalDirectory, configurationManager.DefaultConfiguration.LowWaterMark)
	memtable = memTable.NewMemTable(configurationManager.DefaultConfiguration.MemTableThreshold, configurationManager.DefaultConfiguration.MemTableCapacity)
	lsm = LSMTree.NewLSM(4, configurationManager.DefaultConfiguration.GetLSMDirectory())
	lruCache = lru.NewLRU(configurationManager.DefaultConfiguration.GetCacheCapacity())

	fmt.Println(string(get("1054")))
	fmt.Println(string(get("1054")))
}

func get(key string) []byte {
	value, err := memtable.Get(key)
	if err == nil {
		lruCache.Set(key, value)
		return value
	}

	value = lruCache.Get(key)
	if value != nil {
		return value
	}

	levelFolders, err := ioutil.ReadDir(lsm.DirPath())
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(levelFolders); i++ {
		SSTablesFolders, err := ioutil.ReadDir(lsm.DirPath() + "/" + levelFolders[i].Name())
		if err != nil {
			log.Fatal(err)
		}

		for j := len(SSTablesFolders) - 1; j >= 0; j-- {
			index := strings.LastIndex(SSTablesFolders[j].Name(), "_")
			num := SSTablesFolders[j].Name()[index+1 : len(SSTablesFolders[j].Name())]
			bloomName := lsm.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Filter.bin"
			bloomFile, _ := os.ReadFile(bloomName)
			bloom := bloomFilter.Decode(bloomFile)

			if !bloom.Contains([]byte(key)) {
				continue
			} else {
				summaryName := lsm.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Summary.bin"
				summaryFile, _ := os.OpenFile(summaryName, os.O_RDONLY, 0665+1)

				firstKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
				err := binary.Read(summaryFile, binary.LittleEndian, &firstKeySize)
				if err != nil {
					log.Fatal()
				}

				firstKey := make([]byte, binary.LittleEndian.Uint64(firstKeySize), binary.LittleEndian.Uint64(firstKeySize))
				err = binary.Read(summaryFile, binary.LittleEndian, &firstKey)
				if err != nil {
					log.Fatal()
				}

				lastKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
				err = binary.Read(summaryFile, binary.LittleEndian, &lastKeySize)
				if err != nil {
					log.Fatal()
				}

				lastKey := make([]byte, binary.LittleEndian.Uint64(lastKeySize), binary.LittleEndian.Uint64(lastKeySize))
				err = binary.Read(summaryFile, binary.LittleEndian, &lastKey)
				if err != nil {
					log.Fatal()
				}

				if string(firstKey) <= key && key <= string(lastKey) {
					for {
						currentKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
						err := binary.Read(summaryFile, binary.LittleEndian, &currentKeySize)
						if err != nil {
							log.Fatal()
						}

						currentKey := make([]byte, binary.LittleEndian.Uint64(currentKeySize), binary.LittleEndian.Uint64(currentKeySize))
						err = binary.Read(summaryFile, binary.LittleEndian, &currentKey)
						if err != nil {
							log.Fatal()
						}

						if string(currentKey) > key {
							break
						}

						var currentAddress uint64
						err = binary.Read(summaryFile, binary.LittleEndian, &currentAddress)
						if err != nil {
							log.Fatal()
						}

						if key == string(currentKey) {
							indexName := lsm.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Index.bin"
							indexFile, _ := os.OpenFile(indexName, os.O_RDONLY, 0665+1)

							_, err = indexFile.Seek(int64(currentAddress), 0)
							if err != nil {
								return nil
							}

							indexKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
							err := binary.Read(indexFile, binary.LittleEndian, &indexKeySize)
							if err != nil {
								log.Fatal()
							}

							indexKey := make([]byte, binary.LittleEndian.Uint64(indexKeySize), binary.LittleEndian.Uint64(indexKeySize))
							err = binary.Read(indexFile, binary.LittleEndian, &indexKey)
							if err != nil {
								log.Fatal()
							}

							var currentIndexAddress uint64
							err = binary.Read(indexFile, binary.LittleEndian, &currentIndexAddress)
							if err != nil {
								log.Fatal()
							}

							indexFile.Close()
							dataName := lsm.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Data.bin"
							dataFile, _ := os.OpenFile(dataName, os.O_RDONLY, 0665+1)

							_, err = dataFile.Seek(int64(currentIndexAddress), 0)
							if err != nil {
								return nil
							}

							crc := make([]byte, recordUtil.CRC_SIZE, recordUtil.CRC_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &crc)
							if err != nil {
								log.Fatal()
							}

							tst := make([]byte, recordUtil.TIMESTAMP_SIZE, recordUtil.TIMESTAMP_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &tst)
							if err != nil {
								log.Fatal()
							}

							tStone := make([]byte, recordUtil.TOMBSTONE_SIZE, recordUtil.TOMBSTONE_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &tStone)
							if err != nil {
								log.Fatal()
							}

							keySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &keySize)
							if err != nil {
								log.Fatal()
							}

							valSize := make([]byte, recordUtil.VALUE_SIZE, recordUtil.VALUE_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &valSize)
							if err != nil {
								log.Fatal()
							}

							newKey := make([]byte, binary.LittleEndian.Uint64(keySize), binary.LittleEndian.Uint64(keySize))
							err = binary.Read(dataFile, binary.LittleEndian, &newKey)
							if err != nil {
								log.Fatal()
							}

							value := make([]byte, binary.LittleEndian.Uint64(valSize), binary.LittleEndian.Uint64(valSize))
							err = binary.Read(dataFile, binary.LittleEndian, &value)
							if err != nil {
								log.Fatal()
							}

							if binary.LittleEndian.Uint32(crc) != recordUtil.CRC32(value) {
								log.Fatal()
							}

							lruCache.Set(key, value)
							return value
						}
					}
					summaryFile.Close()
				}
			}
		}
	}

	return nil
}
