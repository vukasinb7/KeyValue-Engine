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
	"pair"
	"recordUtil"
	"strings"
	"time"
	"tokenBucket"
	"wal"
)

func insertTestData() {
	data := configurationManager.ParseData(configurationManager.Configuration.DataFile)
	for _, val := range data {
		err := w.PushRecord(val)
		if err != nil {
			log.Fatal(err)
		}
		memtable.Insert(val)
		lruCache.Set(val.Key, val.Value, val.Tombstone)
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
var tb tokenBucket.TokenBucket

func main() {
	configurationManager.LoadDefaultConfiguration("Data/userConfiguration.json")
	fmt.Println(configurationManager.Configuration.WalSegmentSize)
	w = wal.CreateWal(configurationManager.Configuration.WalSegmentSize, configurationManager.Configuration.WalDirectory, configurationManager.Configuration.LowWaterMark)
	memtable = memTable.NewMemTable(configurationManager.Configuration.MemTableThreshold, configurationManager.Configuration.MemTableCapacity)
	lsm = LSMTree.NewLSM(configurationManager.Configuration.GetLSMlevelNum(), configurationManager.Configuration.GetLSMDirectory())
	lruCache = lru.NewLRU(configurationManager.Configuration.GetCacheCapacity())
	tb = tokenBucket.NewTokenBucket(configurationManager.Configuration.GetTokenBucketNumOfTries(), configurationManager.Configuration.GetTokenBucketInterval())

	//fmt.Println(Get("1001"))
}

func Delete(key string) bool {
	value := Get(key)
	if tb.CheckInputTimer() {
		if value != nil {
			tombstone := byte(1)
			currentTime := time.Now()
			timestamp := currentTime.UnixNano()
			newPair := pair.KVPair{Key: key, Value: value, Tombstone: tombstone, Timestamp: uint64(timestamp)}

			err := w.PushRecord(newPair)
			if err != nil {
				log.Fatal(err)
			}
			memtable.Delete(newPair.Key)
			lruCache.Set(key, value, tombstone)
			if memtable.Size() > memtable.Threshold() {
				lsm.CreateLevelTables(memtable.Flush())
				w.ResetWAL()
			}
			return true
		} else {
			log.Println("Record not found")
			return false
		}

	} else {
		log.Println("Too many inputs")
		return false
	}
}

func Put(key string, value []byte) bool {
	if tb.CheckInputTimer() {
		tombstone := byte(0)
		currentTime := time.Now()
		timestamp := currentTime.UnixNano()
		newPair := pair.KVPair{key, value, tombstone, uint64(timestamp)}

		err := w.PushRecord(newPair)
		if err != nil {
			log.Fatal(err)
		}
		memtable.Insert(newPair)
		lruCache.Set(key, value, tombstone)
		if memtable.Size() > memtable.Threshold() {
			lsm.CreateLevelTables(memtable.Flush())
			w.ResetWAL()
		}
		return true
	} else {
		log.Println("Too many inputs")
		return false
	}
}

func Get(key string) []byte {
	value, tombstone, err := memtable.Get(key)
	if err == nil && tombstone == 0 {
		lruCache.Set(key, value, tombstone)
		return value
	} else if err == nil && tombstone == 1 {
		lruCache.Set(key, value, tombstone)
		return nil
	}

	value, tombstone = lruCache.Get(key)
	if value != nil && tombstone == 0 {
		return value
	} else if value != nil && tombstone == 1 {
		return nil
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

						var currentIndexAddress uint64
						err = binary.Read(summaryFile, binary.LittleEndian, &currentIndexAddress)
						if err != nil {
							log.Fatal()
						}

						if key == string(currentKey) {
							indexName := lsm.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Index.bin"
							indexFile, _ := os.OpenFile(indexName, os.O_RDONLY, 0665+1)

							_, err = indexFile.Seek(int64(currentIndexAddress), 0)
							if err != nil {
								return nil
							}

							_, err = indexFile.Seek(recordUtil.KEY_SIZE, 1)
							if err != nil {
								return nil
							}
							_, err = indexFile.Seek(int64(binary.LittleEndian.Uint64(currentKeySize)), 1)
							if err != nil {
								return nil
							}

							var dataAddress uint64
							err = binary.Read(indexFile, binary.LittleEndian, &dataAddress)
							if err != nil {
								log.Fatal()
							}

							indexFile.Close()
							dataName := lsm.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Data.bin"
							dataFile, _ := os.OpenFile(dataName, os.O_RDONLY, 0665+1)

							_, err = dataFile.Seek(int64(dataAddress), 0)
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

							_, err = dataFile.Seek(recordUtil.KEY_SIZE, 1)
							if err != nil {
								return nil
							}

							valSize := make([]byte, recordUtil.VALUE_SIZE, recordUtil.VALUE_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &valSize)
							if err != nil {
								log.Fatal()
							}

							_, err = dataFile.Seek(int64(binary.LittleEndian.Uint64(currentKeySize)), 1)
							if err != nil {
								return nil
							}

							value := make([]byte, binary.LittleEndian.Uint64(valSize), binary.LittleEndian.Uint64(valSize))
							err = binary.Read(dataFile, binary.LittleEndian, &value)
							if err != nil {
								log.Fatal()
							}

							if binary.LittleEndian.Uint32(crc) != recordUtil.CRC32(value) {
								log.Fatal()
							}

							lruCache.Set(key, value, tStone[0])
							if tStone[0] == 0 {
								return value
							}
						}
					}
					summaryFile.Close()
				}
			}
		}
	}

	return nil
}
