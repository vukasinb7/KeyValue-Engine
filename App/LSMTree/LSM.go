package LSMTree

import (
	"SSTable"
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	"log"
	"math"
	"os"
	"pair"
	"recordUtil"
	"strconv"
)

type LSM struct {
	lsmLevels LSMlevel
	maxLvl    uint32
	dirPath   string
}

type LSMlevel struct {
	manager   *SSTable.SSTableManager
	nextLevel *LSMlevel
	size      uint64
	threshold uint64
}

func (lsmLvl *LSMlevel) compaction() {
	levelFolders, err := ioutil.ReadDir(lsmLvl.manager.DirPath())
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(levelFolders); i += 2 {
		if i+1 < len(levelFolders) {
			data1, err := os.OpenFile(lsmLvl.manager.DirPath()+"/"+levelFolders[i].Name()+"/Usertable-"+strconv.Itoa(i+1)+"-Data.bin", os.O_RDONLY, 0663+3)
			if err != nil {
				log.Fatal(err)
			}

			data2, err := os.OpenFile(lsmLvl.manager.DirPath()+"/"+levelFolders[i+1].Name()+"/Usertable-"+strconv.Itoa(i+2)+"-Data.bin", os.O_RDONLY, 0663+3)
			if err != nil {
				log.Fatal(err)
			}

			rec1, err := readRecord(data1)
			rec2, err := readRecord(data2)

			var pairs []pair.KVPair
			for {
				if rec1.Key < rec2.Key {
					pairs = append(pairs, rec1)
					rec1, err = readRecord(data1)
					if err != nil {
						break
					}
				} else if rec1.Key > rec2.Key {
					pairs = append(pairs, rec2)
					rec2, err = readRecord(data2)
					if err != nil {
						break
					}
				} else {
					pairs = append(pairs, rec2)
					rec1, err = readRecord(data1)
					if err != nil {
						break
					}
					rec2, err = readRecord(data2)
					if err != nil {
						break
					}
				}
			}
			for {
				rec1, err = readRecord(data1)
				if err != nil {
					break
				}
				pairs = append(pairs, rec1)
			}
			for {
				rec2, err = readRecord(data2)
				if err != nil {
					break
				}
				pairs = append(pairs, rec2)
			}

			data1_stat, _ := data1.Stat()
			data2_stat, _ := data2.Stat()
			lsmLvl.size -= uint64(data1_stat.Size())
			lsmLvl.size -= uint64(data2_stat.Size())

			lsmLvl.nextLevel.createSSTable(pairs)
			err = data1.Close()
			if err != nil {
				log.Fatal(err)
			}
			err = data2.Close()
			if err != nil {
				log.Fatal(err)
			}
			err = os.RemoveAll(lsmLvl.manager.DirPath() + "/" + levelFolders[i].Name())
			if err != nil {
				log.Fatal(err)
			}
			err = os.RemoveAll(lsmLvl.manager.DirPath() + "/" + levelFolders[i+1].Name())
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func readRecord(file *os.File) (pair.KVPair, error) {
	crc := make([]byte, recordUtil.CRC_SIZE, recordUtil.CRC_SIZE)
	err := binary.Read(file, binary.LittleEndian, &crc)
	if err != nil {
		return pair.KVPair{}, err
	}

	tst := make([]byte, recordUtil.TIMESTAMP_SIZE, recordUtil.TIMESTAMP_SIZE)
	err = binary.Read(file, binary.LittleEndian, &tst)
	if err != nil {
		return pair.KVPair{}, err
	}

	tStone := make([]byte, recordUtil.TOMBSTONE_SIZE, recordUtil.TOMBSTONE_SIZE)
	err = binary.Read(file, binary.LittleEndian, &tStone)
	if err != nil {
		return pair.KVPair{}, err
	}

	keySize := make([]byte, recordUtil.KEY_SIZE, recordUtil.KEY_SIZE)
	err = binary.Read(file, binary.LittleEndian, &keySize)
	if err != nil {
		return pair.KVPair{}, err
	}

	valSize := make([]byte, recordUtil.VALUE_SIZE, recordUtil.VALUE_SIZE)
	err = binary.Read(file, binary.LittleEndian, &valSize)
	if err != nil {
		return pair.KVPair{}, err
	}

	key := make([]byte, binary.LittleEndian.Uint64(keySize), binary.LittleEndian.Uint64(keySize))
	err = binary.Read(file, binary.LittleEndian, &key)
	if err != nil {
		return pair.KVPair{}, err
	}

	value := make([]byte, binary.LittleEndian.Uint64(valSize), binary.LittleEndian.Uint64(valSize))
	err = binary.Read(file, binary.LittleEndian, &value)
	if err != nil {
		return pair.KVPair{}, err
	}

	if binary.LittleEndian.Uint32(crc) != CRC32(value) {
		return pair.KVPair{}, err
	}

	return pair.KVPair{Key: string(key), Value: value, Tombstone: tStone[0], Timestamp: binary.LittleEndian.Uint64(tst)}, nil
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (lsmLvl *LSMlevel) createSSTable(pairs []pair.KVPair) {
	size, err := lsmLvl.manager.CreateSSTable(pairs)
	if err != nil {
		return
	}
	lsmLvl.size += size
	if lsmLvl.size > lsmLvl.threshold {
		if lsmLvl.nextLevel != nil {
			lsmLvl.compaction()
		}
	}
}

func (lsm *LSM) CreateLevelTables(pairs []pair.KVPair) {
	lsm.lsmLevels.createSSTable(pairs)
}

func NewLSM(maxLvl uint32, dirPath string) LSM {
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
		Levelfiles, err := ioutil.ReadDir(dirPath + levels[i].Name())
		if err != nil {
			log.Fatal(err)
		}

		size := uint64(0)
		for j := 0; j < len(Levelfiles); j++ {
			fileStat, _ := os.Stat(dirPath + levels[i].Name() + "/" + Levelfiles[j].Name() + "/Usertable-" + strconv.Itoa(j+1) + "-Data.bin")
			size += uint64(fileStat.Size())
		}
		tempLevel := LSMlevel{
			manager:   SSTable.CreateSSTableMng(dirPath + levels[i].Name()),
			size:      size,
			threshold: uint64(baseThreshold * math.Pow(10, float64(i))),
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
