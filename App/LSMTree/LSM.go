package LSMTree

import (
	"SSTable"
	"encoding/binary"
	"io/ioutil"
	"log"
	"math"
	"os"
	"pair"
	"recordUtil"
	"sort"
	"strconv"
	"strings"
)

type LSM struct {
	lsmLevels LSMlevel
	maxLvl    uint32
	dirPath   string
}

func (lsm *LSM) LsmLevels() *LSMlevel {
	return &lsm.lsmLevels
}

func (lsm *LSM) DirPath() string {
	return lsm.dirPath
}

type LSMlevel struct {
	manager   *SSTable.SSTableManager
	nextLevel *LSMlevel
	size      uint64
	threshold uint64
}

func (lsmLvl *LSMlevel) NextLevel() *LSMlevel {
	return lsmLvl.nextLevel
}

func (lsmLvl *LSMlevel) Size() uint64 {
	return lsmLvl.size
}

func (lsmLvl *LSMlevel) Threshold() uint64 {
	return lsmLvl.threshold
}

func (lsmLvl *LSMlevel) Compaction() {
	levelFolders, err := ioutil.ReadDir(lsmLvl.manager.DirPath())
	if err != nil {
		log.Fatal(err)
	}

	sort.Slice(levelFolders, func(y, z int) bool {
		index1 := strings.LastIndex(levelFolders[y].Name(), "_")
		num1 := levelFolders[y].Name()[index1+1 : len(levelFolders[y].Name())]

		index2 := strings.LastIndex(levelFolders[z].Name(), "_")
		num2 := levelFolders[z].Name()[index2+1 : len(levelFolders[z].Name())]

		a, _ := strconv.Atoi(num1)
		b, _ := strconv.Atoi(num2)
		return a < b
	})

	for i := 0; i < len(levelFolders); i += 2 {
		if i+1 < len(levelFolders) {
			index := strings.LastIndex(levelFolders[i].Name(), "_")
			num := levelFolders[i].Name()[index+1 : len(levelFolders[i].Name())]
			data1, err := os.OpenFile(lsmLvl.manager.DirPath()+"/"+levelFolders[i].Name()+"/Usertable-"+num+"-Data.bin", os.O_RDONLY, 0663+3)
			if err != nil {
				log.Fatal(err)
			}
			index = strings.LastIndex(levelFolders[i+1].Name(), "_")
			num = levelFolders[i+1].Name()[index+1 : len(levelFolders[i+1].Name())]
			data2, err := os.OpenFile(lsmLvl.manager.DirPath()+"/"+levelFolders[i+1].Name()+"/Usertable-"+num+"-Data.bin", os.O_RDONLY, 0663+3)
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

			lsmLvl.nextLevel.createSSTableWithAutomation(pairs)
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

	if binary.LittleEndian.Uint32(crc) != recordUtil.CRC32(value) {
		return pair.KVPair{}, err
	}

	return pair.KVPair{Key: string(key), Value: value, Tombstone: tStone[0], Timestamp: binary.LittleEndian.Uint64(tst)}, nil
}

func (lsmLvl *LSMlevel) createSSTable(pairs []pair.KVPair) {
	size, err := lsmLvl.manager.CreateSSTable(pairs)
	if err != nil {
		return
	}
	lsmLvl.size += size
}

func (lsmLvl *LSMlevel) createSSTableWithAutomation(pairs []pair.KVPair) {
	size, err := lsmLvl.manager.CreateSSTable(pairs)
	if err != nil {
		return
	}
	lsmLvl.size += size
	if lsmLvl.size > lsmLvl.threshold {
		if lsmLvl.nextLevel != nil {
			lsmLvl.Compaction()
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

	for i := 0; i < int(maxLvl); i++ {
		_ = os.MkdirAll(dirPath+"/C"+strconv.Itoa(i+1), os.ModePerm)
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
			index := strings.LastIndex(Levelfiles[j].Name(), "_")
			num := Levelfiles[j].Name()[index+1 : len(Levelfiles[j].Name())]
			fileStat, _ := os.Stat(dirPath + levels[i].Name() + "/" + Levelfiles[j].Name() + "/Usertable-" + num + "-Data.bin")
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
