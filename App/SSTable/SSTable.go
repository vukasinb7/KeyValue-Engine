package SSTable

import (
	"bloomFilter"
	"encoding/binary"
	"io/ioutil"
	"log"
	"merkleTree"
	"os"
	"pair"
	"recordUtil"
	"strconv"
	"strings"
)

type SSTableManager struct {
	currentIndex uint32
	dirPath      string
}

func (ss *SSTableManager) DirPath() string {
	return ss.dirPath
}

func CreateSSTableMng(DirPath string) *SSTableManager {
	tableFolders, err := ioutil.ReadDir(DirPath)
	if err != nil {
		log.Fatal(err)
	}

	num2, _ := strconv.ParseInt("1", 10, 32)
	currentIndex := uint32(num2)
	if len(tableFolders) > 0 {
		maxNum := uint32(0)
		for i := 0; i < len(tableFolders); i++ {
			index := strings.LastIndex(tableFolders[i].Name(), "_")
			num := tableFolders[i].Name()[index+1 : len(tableFolders[i].Name())]
			temp, _ := strconv.ParseInt(num, 10, 32)
			if uint32(temp) > maxNum {
				maxNum = uint32(temp)
			}

		}
		currentIndex = maxNum + 1
	}

	ss := SSTableManager{
		currentIndex: currentIndex,
		dirPath:      DirPath,
	}

	return &ss
}
func (ss *SSTableManager) CreateSSTable(pairs []pair.KVPair) (uint64, error) {
	folderName := ss.dirPath + "/SSTable_" + strconv.Itoa(int(ss.currentIndex))
	err := os.Mkdir(folderName, 0664+2)
	if err != nil {
		return 0, err
	}
	indexFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Index.bin")
	if err != nil {
		return 0, err
	}
	metadataFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Metadata.txt")
	if err != nil {
		return 0, err
	}
	summaryFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Summary.bin")
	if err != nil {
		return 0, err
	}
	dataFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Data.bin")
	if err != nil {
		return 0, err
	}
	tocFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-TOC.txt")
	if err != nil {
		return 0, err
	}
	filterFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Filter.bin")
	if err != nil {
		return 0, err
	}

	firstKeySize := len(pairs[0].Key)
	lastKeySize := len(pairs[len(pairs)-1].Key)
	headerSize := 2*recordUtil.KEY_SIZE + firstKeySize + lastKeySize
	headerRecord := make([]byte, headerSize, headerSize)
	binary.LittleEndian.PutUint64(headerRecord[:], uint64(firstKeySize))
	for i := 0; i < firstKeySize; i++ {
		headerRecord[recordUtil.KEY_SIZE+i] = pairs[0].Key[i]
	}
	binary.LittleEndian.PutUint64(headerRecord[recordUtil.KEY_SIZE+firstKeySize:], uint64(lastKeySize))
	for i := 0; i < lastKeySize; i++ {
		headerRecord[recordUtil.KEY_SIZE*2+firstKeySize+i] = pairs[len(pairs)-1].Key[i]
	}
	_, err = summaryFile.Write(headerRecord)
	if err != nil {
		return 0, err
	}

	var merkleTreeData [][]byte
	bloom := bloomFilter.NewBloomFilter(0.001, len(pairs))

	for _, record := range pairs {
		recordSize := recordUtil.CRC_SIZE + recordUtil.TOMBSTONE_SIZE + recordUtil.TIMESTAMP_SIZE + recordUtil.KEY_SIZE + recordUtil.VALUE_SIZE + len(record.Key) + len(record.Value)
		newRecord := make([]byte, recordSize, recordSize)

		crc := recordUtil.CRC32(record.Value)

		binary.LittleEndian.PutUint32(newRecord[:], crc)
		binary.LittleEndian.PutUint64(newRecord[recordUtil.CRC_SIZE:], record.Timestamp)

		newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE] = record.Tombstone

		binary.LittleEndian.PutUint64(newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE:], uint64(len(record.Key)))
		binary.LittleEndian.PutUint64(newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE+recordUtil.KEY_SIZE:], uint64(len(record.Value)))
		for i := 0; i < len(record.Key); i++ {
			newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE+recordUtil.KEY_SIZE+recordUtil.VALUE_SIZE+i] = record.Key[i]
		}
		for i := 0; i < len(record.Value); i++ {
			newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE+recordUtil.KEY_SIZE+recordUtil.VALUE_SIZE+len(record.Key)+i] = record.Value[i]
		}

		address, err := dataFile.Seek(0, 1)
		if err != nil {
			return 0, err
		}

		_, err = dataFile.Write(newRecord)
		merkleTreeData = append(merkleTreeData, newRecord)
		if err != nil {
			return 0, err
		}

		indexSize := recordUtil.KEY_SIZE + len(record.Key) + 8
		indexRecord := make([]byte, indexSize, indexSize)
		binary.LittleEndian.PutUint64(indexRecord[:], uint64(len(record.Key)))
		for i := 0; i < len(record.Key); i++ {
			indexRecord[recordUtil.KEY_SIZE+i] = record.Key[i]
		}
		binary.LittleEndian.PutUint64(indexRecord[recordUtil.KEY_SIZE+len(record.Key):], uint64(address))
		indexAddress, err := indexFile.Seek(0, 1)
		_, err = indexFile.Write(indexRecord)
		if err != nil {
			return 0, err
		}

		summarySize := recordUtil.KEY_SIZE + len(record.Key) + 8
		summaryRecord := make([]byte, summarySize, summarySize)
		binary.LittleEndian.PutUint64(summaryRecord[:], uint64(len(record.Key)))
		for i := 0; i < len(record.Key); i++ {
			summaryRecord[recordUtil.KEY_SIZE+i] = record.Key[i]
		}
		binary.LittleEndian.PutUint64(summaryRecord[recordUtil.KEY_SIZE+len(record.Key):], uint64(indexAddress))
		_, err = summaryFile.Write(summaryRecord)
		if err != nil {
			return 0, err
		}

		bloom.Insert([]byte(record.Key))
	}
	bloomBytes := bloom.Encode()
	_, err = filterFile.Write(bloomBytes)
	if err != nil {
		return 0, err
	}

	mr := merkleTree.NewMerkleTree(merkleTreeData)
	mr.SerializeMerkleTree(metadataFile)
	tocFile.Write([]byte(dataFile.Name() + "\n"))
	tocFile.Write([]byte(indexFile.Name() + "\n"))
	tocFile.Write([]byte(summaryFile.Name() + "\n"))
	tocFile.Write([]byte(filterFile.Name() + "\n"))
	tocFile.Write([]byte(metadataFile.Name() + "\n"))
	indexFile.Close()
	dataFile.Close()
	filterFile.Close()
	tocFile.Close()
	summaryFile.Close()
	metadataFile.Close()

	stat, _ := os.Stat(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Data.bin")
	dataLength := stat.Size()
	ss.currentIndex++

	return uint64(dataLength), nil

}
