package Engine

import (
	"LSMTree"
	"bloomFilter"
	"countMinSketch"
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

var DefMemtable memTable.MemTable
var DefWal wal.Wal
var DefLSM LSMTree.LSM
var DefLRUCache lru.LRUCache
var DefTB tokenBucket.TokenBucket

func Delete(key string) bool {
	if DefTB.CheckInputTimer() {
		tombstone := byte(1)
		currentTime := time.Now()
		timestamp := currentTime.UnixNano()
		newPair := pair.KVPair{Key: key, Value: []byte{}, Tombstone: tombstone, Timestamp: uint64(timestamp)}

		err := DefWal.PushRecord(newPair)
		if err != nil {
			log.Fatal(err)
		}
		DefMemtable.Delete(newPair.Key)
		DefLRUCache.Set(key, []byte{}, tombstone)
		if DefMemtable.Size() > DefMemtable.Threshold() {
			DefLSM.CreateLevelTables(DefMemtable.Flush())
			DefWal.ResetWAL()
		}
		fmt.Println("\nRecord deleted successfully!")
		return true
	} else {
		fmt.Println("\nToo many inputs!")
		return false
	}
}

func Put(key string, value []byte) bool {
	if DefTB.CheckInputTimer() {
		tombstone := byte(0)
		currentTime := time.Now()
		timestamp := currentTime.UnixNano()
		newPair := pair.KVPair{key, value, tombstone, uint64(timestamp)}

		err := DefWal.PushRecord(newPair)
		if err != nil {
			log.Fatal(err)
		}
		DefMemtable.Insert(newPair)
		DefLRUCache.Set(key, value, tombstone)
		if DefMemtable.Size() > DefMemtable.Threshold() {
			DefLSM.CreateLevelTables(DefMemtable.Flush())
			DefWal.ResetWAL()
		}
		fmt.Println("\nRecord added successfully!")
		return true
	} else {
		fmt.Println("\nToo many inputs!")
		return false
	}
}

func Get(key string) []byte {
	value, tombstone, err := DefMemtable.Get(key)
	if err == nil && tombstone == 0 {
		DefLRUCache.Set(key, value, tombstone)
		return value
	} else if err == nil && tombstone == 1 {
		DefLRUCache.Set(key, value, tombstone)
		return nil
	}

	value, tombstone = DefLRUCache.Get(key)
	if value != nil && tombstone == 0 {
		return value
	} else if value != nil && tombstone == 1 {
		return nil
	}

	levelFolders, err := ioutil.ReadDir(DefLSM.DirPath())
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(levelFolders); i++ {
		SSTablesFolders, err := ioutil.ReadDir(DefLSM.DirPath() + "/" + levelFolders[i].Name())
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
			bloomName := DefLSM.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Filter.bin"
			bloomFile, _ := os.ReadFile(bloomName)
			bloom := bloomFilter.Decode(bloomFile)

			if !bloom.Contains([]byte(key)) {
				continue
			} else {
				summaryName := DefLSM.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Summary.bin"
				summaryFile, _ := os.OpenFile(summaryName, os.O_RDONLY, 0665+1)

				firstKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
				err := binary.Read(summaryFile, binary.LittleEndian, &firstKeySize)
				if err != nil {
					log.Fatal(err)
				}

				firstKey := make([]byte, binary.LittleEndian.Uint64(firstKeySize), binary.LittleEndian.Uint64(firstKeySize))
				err = binary.Read(summaryFile, binary.LittleEndian, &firstKey)
				if err != nil {
					log.Fatal(err)
				}

				lastKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
				err = binary.Read(summaryFile, binary.LittleEndian, &lastKeySize)
				if err != nil {
					log.Fatal(err)
				}

				lastKey := make([]byte, binary.LittleEndian.Uint64(lastKeySize), binary.LittleEndian.Uint64(lastKeySize))
				err = binary.Read(summaryFile, binary.LittleEndian, &lastKey)
				if err != nil {
					log.Fatal(err)
				}

				if string(firstKey) <= key && key <= string(lastKey) {
					for {
						currentKeySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
						err := binary.Read(summaryFile, binary.LittleEndian, &currentKeySize)
						if err != nil {
							log.Fatal(err)
						}

						currentKey := make([]byte, binary.LittleEndian.Uint64(currentKeySize), binary.LittleEndian.Uint64(currentKeySize))
						err = binary.Read(summaryFile, binary.LittleEndian, &currentKey)
						if err != nil {
							log.Fatal(err)
						}

						if string(currentKey) > key {
							break
						}

						var currentIndexAddress uint64
						err = binary.Read(summaryFile, binary.LittleEndian, &currentIndexAddress)
						if err != nil {
							log.Fatal(err)
						}

						if key == string(currentKey) {
							indexName := DefLSM.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Index.bin"
							indexFile, _ := os.OpenFile(indexName, os.O_RDONLY, 0665+1)

							_, err = indexFile.Seek(int64(currentIndexAddress), 0)
							if err != nil {
								log.Fatal(err)
							}

							_, err = indexFile.Seek(recordUtil.KEY_SIZE, 1)
							if err != nil {
								log.Fatal(err)
							}
							_, err = indexFile.Seek(int64(binary.LittleEndian.Uint64(currentKeySize)), 1)
							if err != nil {
								log.Fatal(err)
							}

							var dataAddress uint64
							err = binary.Read(indexFile, binary.LittleEndian, &dataAddress)
							if err != nil {
								log.Fatal(err)
							}

							err := indexFile.Close()
							if err != nil {
								log.Fatal(err)
							}
							dataName := DefLSM.DirPath() + "/" + levelFolders[i].Name() + "/" + SSTablesFolders[j].Name() + "/Usertable-" + num + "-Data.bin"
							dataFile, _ := os.OpenFile(dataName, os.O_RDONLY, 0665+1)

							_, err = dataFile.Seek(int64(dataAddress), 0)
							if err != nil {
								log.Fatal(err)
							}

							crc := make([]byte, recordUtil.CRC_SIZE, recordUtil.CRC_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &crc)
							if err != nil {
								log.Fatal(err)
							}

							tst := make([]byte, recordUtil.TIMESTAMP_SIZE, recordUtil.TIMESTAMP_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &tst)
							if err != nil {
								log.Fatal(err)
							}

							tStone := make([]byte, recordUtil.TOMBSTONE_SIZE, recordUtil.TOMBSTONE_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &tStone)
							if err != nil {
								log.Fatal(err)
							}

							_, err = dataFile.Seek(recordUtil.KEY_SIZE, 1)
							if err != nil {
								log.Fatal(err)
							}

							valSize := make([]byte, recordUtil.VALUE_SIZE, recordUtil.VALUE_SIZE)
							err = binary.Read(dataFile, binary.LittleEndian, &valSize)
							if err != nil {
								log.Fatal(err)
							}

							_, err = dataFile.Seek(int64(binary.LittleEndian.Uint64(currentKeySize)), 1)
							if err != nil {
								log.Fatal(err)
							}

							value := make([]byte, binary.LittleEndian.Uint64(valSize), binary.LittleEndian.Uint64(valSize))
							err = binary.Read(dataFile, binary.LittleEndian, &value)
							if err != nil {
								log.Fatal(err)
							}

							if binary.LittleEndian.Uint32(crc) != recordUtil.CRC32(value) {
								log.Fatal(err)
							}
							err = dataFile.Close()
							if err != nil {
								return nil
							}
							err = summaryFile.Close()
							if err != nil {
								return nil
							}

							DefLRUCache.Set(key, value, tStone[0])
							if tStone[0] == 0 {
								return value
							} else {
								return nil
							}
						}
					}
					err = summaryFile.Close()
					if err != nil {
						return nil
					}
				}
			}
		}
	}

	return nil
}

func CreateHll(key string, values [][]byte, p uint) error {
	// ================
	// Description:
	// ================
	// 		Creates instance of HyperLogLog and inserts values into it
	//		Stores the structure in database with the given key
	//		Returns error if the key already exists
	check := Get(key)
	if check != nil {
		return errors.New("key already in database")
	} else {
		hll := hyperLogLog.NewHyperLogLog(p)
		for _, val := range values {
			hll.Insert(val)
		}
		Put(key, hll.Encode())
	}
	return nil
}

func CreateCms(key string, values [][]byte, prs, acc float64) error {
	check := Get(key)
	if check != nil {
		return errors.New("key already in database")
	} else {
		cms := countMinSketch.NewCountMinSketch(prs, acc)
		for _, val := range values {
			cms.Insert(val)
		}
		Put(key, cms.Encode())
	}
	return nil
}

func InsertIntoCms(key string, values [][]byte) error {
	bytes := Get(key)
	if bytes == nil {
		return errors.New("key not found")
	}
	cms := countMinSketch.Decode(bytes)
	for _, val := range values {
		cms.Insert(val)
	}
	Put(key, cms.Encode())
	return nil
}

func CmsNumOfAppearances(key string, value []byte) (uint, error) {
	bytes := Get(key)
	if bytes == nil {
		return 0, errors.New("key not found")
	}
	var cms countMinSketch.CountMinSketch
	if err := recordUtil.TryCatch(func() { cms = countMinSketch.Decode(bytes) })(); err != nil {
		return 0, errors.New("CMS not found")
	}

	return cms.Count(value), nil
}

func InsertIntoHll(key string, values [][]byte) error {
	// ================
	// Description:
	// ================
	// 		Inserts values into the HyperLogLog with the given key
	//		Returns error if the key is not corresponding to a HyperLogLog structure
	bytes := Get(key)
	if bytes == nil {
		return errors.New("key not found")
	}
	hll := hyperLogLog.Decode(bytes)
	for _, val := range values {
		hll.Insert(val)
	}
	Put(key, hll.Encode())
	return nil
}

func GetCardinality(key string) (float64, error) {
	// ================
	// Description:
	// ================
	// 		Returns cardinality of HyperLogLog with given key
	//		Returns error if the key is not corresponding to a HyperLogLog structure
	bytes := Get(key)
	if bytes == nil {
		return -1, errors.New("key not found")
	}
	var hll hyperLogLog.HyperLogLog
	if err := recordUtil.TryCatch(func() { hll = hyperLogLog.Decode(bytes) })(); err != nil {
		return -1, errors.New("HLL not found")
	}

	return hll.Cardinality(), nil
}

func CreateBloomFilter(key string, values [][]byte, p float64, n int) error {
	check := Get(key)
	if check != nil {
		return errors.New("key already in database")
	} else {
		bFilter := bloomFilter.NewBloomFilter(p, n)
		for _, val := range values {
			bFilter.Insert(val)
		}
		Put(key, bFilter.Encode())
	}
	return nil
}

func InsertIntoBloomFilter(key string, values [][]byte) error {
	bytes := Get(key)
	if bytes == nil {
		return errors.New("key not found")
	}
	bFilter := bloomFilter.Decode(bytes)
	for _, val := range values {
		bFilter.Insert(val)
	}
	Put(key, bFilter.Encode())
	return nil
}

func BloomFilterContains(key string, value []byte) (bool, error) {
	bytes := Get(key)
	if bytes == nil {
		return false, errors.New("key not found")
	}
	var bf bloomFilter.BloomFilter
	if err := recordUtil.TryCatch(func() { bf = bloomFilter.Decode(bytes) })(); err != nil {
		return false, errors.New("BF not found")
	}

	return bf.Contains(value), nil
}
