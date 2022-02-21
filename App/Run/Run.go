package main

import (
	"LSMTree"
	"bloomFilter"
	"bufio"
	"configurationManager"
	"encoding/binary"
	"errors"
	"fmt"
	"hyperLogLog"
	"io/ioutil"
	"log"
	"lru"
	"memTable"
	"os"
	"pair"
	"recordUtil"
	"sort"
	"strconv"
	"strings"
	"time"
	"tokenBucket"
	"wal"
)

func insertTestData() {
	data := configurationManager.ParseTxtData(configurationManager.Configuration.DataFile)
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
	configurationManager.LoadUserConfiguration("Data/userConfiguration.json")
	w = wal.CreateWal(configurationManager.Configuration.WalSegmentSize, configurationManager.Configuration.WalDirectory, configurationManager.Configuration.LowWaterMark)
	memtable = memTable.NewMemTable(configurationManager.Configuration.MemTableThreshold, configurationManager.Configuration.MemTableCapacity)
	lsm = LSMTree.NewLSM(configurationManager.Configuration.GetLSMlevelNum(), configurationManager.Configuration.GetLSMDirectory())
	lruCache = lru.NewLRU(configurationManager.Configuration.GetCacheCapacity())
	tb = tokenBucket.NewTokenBucket(configurationManager.Configuration.GetTokenBucketNumOfTries(), configurationManager.Configuration.GetTokenBucketInterval())
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nMenu")
		fmt.Println("------------------------------")
		fmt.Println("1. Insert data from file")
		fmt.Println("2. Put")
		fmt.Println("3. Get")
		fmt.Println("4. Delete")
		fmt.Println("5. Compactions")
		fmt.Println("6. Exit")
		fmt.Println("------------------------------")
		fmt.Print("Enter option: ")
		var option string
		scanner.Scan()
		option = scanner.Text()
		if option == "1" {
			insertTestData()

		} else if option == "2" {
			fmt.Print("Enter key: ")
			var key string
			scanner.Scan()
			key = scanner.Text()
			fmt.Print("Enter value: ")
			var value []byte
			scanner.Scan()
			value = scanner.Bytes()
			Put(key, value)

		} else if option == "3" {
			fmt.Print("Enter key: ")
			var key string
			scanner.Scan()
			key = scanner.Text()
			value := Get(key)
			if value != nil {
				fmt.Println("\nValue: " + string(value))
			} else {
				fmt.Println("\nRecord not found!")
			}

		} else if option == "4" {
			fmt.Print("Enter key: ")
			var key string
			scanner.Scan()
			key = scanner.Text()
			Delete(key)

		} else if option == "5" {
			head := lsm.LsmLevels()
			if head.Size() > head.Threshold() {
				if head.NextLevel() != nil {
					head.Compaction()
					fmt.Println("\nCompactions done successfully!")
				}
			} else {
				fmt.Println("\nCompactions unsuccessful!")
			}

		} else if option == "6" {
			records := memtable.Flush()
			if len(records) > 0 {
				lsm.CreateLevelTables(records)
			}
			os.Exit(0)
		} else {
			fmt.Println("\nInvalid input!")
		}
	}
}

func CreateHll(key string, values [][]byte, p uint) error{
	// ================
	// Description:
	// ================
	// 		Creates instance of HyperLogLog and inserts values into it
	//		Stores the structure in database with the given key
	//		Returns error if the key already exists
	check := Get(key)
	if check != nil{
		return errors.New("key already in database")
	} else{
		hll := hyperLogLog.NewHyperLogLog(p)
		for _, val := range values{
			hll.Insert(val)
		}
		Put(key, hll.Encode())
	}
	return nil
}
func InsertIntoHll(key string, values [][]byte) error{
	// ================
	// Description:
	// ================
	// 		Inserts values into the HyperLogLog with the given key
	//		Returns error if the key is not corresponding to a HyperLogLog structure
	bytes := Get(key)
	hll := hyperLogLog.Decode(bytes)
	for _, val := range values{
		hll.Insert(val)
	}
	Put(key, hll.Encode())
	return nil
}
func GetCardinality(key string) (float64, error){
	// ================
	// Description:
	// ================
	// 		Returns cardinality of HyperLogLog with given key
	//		Returns error if the key is not corresponding to a HyperLogLog structure
	bytes := Get(key)
	hll := hyperLogLog.Decode(bytes)
	return hll.Cardinality(), nil
}

func CreateBloomFilter(key string, values [][]byte, p float64, n int) error{
	check := Get(key)
	if check != nil{
		return errors.New("key already in database")
	} else{
		bFilter := bloomFilter.NewBloomFilter(p, n)
		for _, val := range values{
			bFilter.Insert(val)
		}
		Put(key, bFilter.Encode())
	}
	return nil
}
func InsertIntoBloomFilter(key string, values [][]byte) error{
	bytes := Get(key)
	bFilter := bloomFilter.Decode(bytes)
	for _, val := range values{
		bFilter.Insert(val)
	}
	Put(key, bFilter.Encode())
	return nil
}
func BloomFilterContains(key string, value []byte) (bool, error){
	bytes := Get(key)
	bFilter := bloomFilter.Decode(bytes)
	return bFilter.Contains(value), nil
}


func Delete(key string) bool {
	if tb.CheckInputTimer() {
		tombstone := byte(1)
		currentTime := time.Now()
		timestamp := currentTime.UnixNano()
		newPair := pair.KVPair{Key: key, Value: []byte{}, Tombstone: tombstone, Timestamp: uint64(timestamp)}

		err := w.PushRecord(newPair)
		if err != nil {
			log.Fatal(err)
		}
		memtable.Delete(newPair.Key)
		lruCache.Set(key, []byte{}, tombstone)
		if memtable.Size() > memtable.Threshold() {
			lsm.CreateLevelTables(memtable.Flush())
			w.ResetWAL()
		}
		fmt.Println("\nRecord deleted successfully!")
		return true
	} else {
		fmt.Println("\nToo many inputs!")
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
		fmt.Println("\nRecord added successfully!")
		return true
	} else {
		fmt.Println("\nToo many inputs!")
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

		sort.Slice(SSTablesFolders, func(y, z int) bool {
			index1 := strings.LastIndex(SSTablesFolders[y].Name(), "_")
			num1 := SSTablesFolders[y].Name()[index1+1 : len(SSTablesFolders[y].Name())]

			index2 := strings.LastIndex(SSTablesFolders[z].Name(), "_")
			num2 := SSTablesFolders[z].Name()[index2+1 : len(SSTablesFolders[z].Name())]

			a, _ := strconv.Atoi(num1)
			b, _ := strconv.Atoi(num2)
			return a < b
		})

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
								log.Fatal()
							}

							_, err = indexFile.Seek(recordUtil.KEY_SIZE, 1)
							if err != nil {
								log.Fatal()
							}
							_, err = indexFile.Seek(int64(binary.LittleEndian.Uint64(currentKeySize)), 1)
							if err != nil {
								log.Fatal()
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
								log.Fatal()
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
								log.Fatal()
							}

							valSize := make([]byte, recordUtil.VALUE_SIZE, recordUtil.VALUE_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &valSize)
							if err != nil {
								log.Fatal()
							}

							_, err = dataFile.Seek(int64(binary.LittleEndian.Uint64(currentKeySize)), 1)
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
							dataFile.Close()
							summaryFile.Close()

							lruCache.Set(key, value, tStone[0])
							if tStone[0] == 0 {
								return value
							} else {
								return nil
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
