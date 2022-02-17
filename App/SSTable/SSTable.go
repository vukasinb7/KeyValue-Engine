package SSTable

import (
	"bloomFilter"
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"pair"
	"recordUtil"
	"strconv"
	"time"
)

/*type SSTable struct {
	IndexPath   string
	SummaryPath string
	FilterPath  string
	DataPath    string
	TOCPath     string
}*/
type SSTableManager struct {
	currentIndex uint32
	dirPath      string
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
func CreateSSTableMng(DirPath string) *SSTableManager {
	tableFolders, err := ioutil.ReadDir(DirPath)
	if err != nil {
		log.Fatal(err)
	}
	currentIndex := len(tableFolders) + 1

	ss := SSTableManager{
		currentIndex: uint32(currentIndex),
		dirPath:      DirPath,
	}

	return &ss
}
func (ss *SSTableManager) CreateSSTable(pairs []pair.KVPair) error {
	N := 10
	folderName := ss.dirPath + "/SSTable_" + strconv.Itoa(int(ss.currentIndex))
	err := os.Mkdir(folderName, 0664+2)
	if err != nil {
		return err
	}
	indexFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Index.bin")
	if err != nil {
		return err
	}

	summaryFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Summary.bin")
	if err != nil {
		return err
	}
	filterFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Filter.bin")
	if err != nil {
		return err
	}
	dataFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-Data.bin")
	if err != nil {
		return err
	}
	tocFile, err := os.Create(folderName + "/Usertable-" + strconv.Itoa(int(ss.currentIndex)) + "-TOC.txt")
	if err != nil {
		return err
	}
	it := 0
	for _, record := range pairs {
		recordSize := recordUtil.CRC_SIZE + recordUtil.TOMBSTONE_SIZE + recordUtil.TIMESTAMP_SIZE + recordUtil.KEY_SIZE + recordUtil.VALUE_SIZE + len(record.Key) + len(record.Value)
		newRecord := make([]byte, recordSize, recordSize)

		crc := recordUtil.CRC32(record.Value)
		currentTime := time.Now()
		timestamp := currentTime.Unix()

		binary.LittleEndian.PutUint32(newRecord[:], crc)
		binary.LittleEndian.PutUint64(newRecord[recordUtil.CRC_SIZE:], uint64(timestamp))

		newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE] = record.Tombstone

		binary.LittleEndian.PutUint64(newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE:], uint64(len(record.Key)))
		binary.LittleEndian.PutUint64(newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE+recordUtil.KEY_SIZE:], uint64(len(record.Value)))
		for i := 0; i < len(record.Key); i++ {
			newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE+recordUtil.KEY_SIZE+recordUtil.VALUE_SIZE+i] = record.Key[i]
		}
		for i := 0; i < len(record.Value); i++ {
			newRecord[recordUtil.CRC_SIZE+recordUtil.TIMESTAMP_SIZE+recordUtil.TOMBSTONE_SIZE+recordUtil.KEY_SIZE+recordUtil.VALUE_SIZE+len(record.Key)+i] = record.Value[i]
		}

		/*_, err := dataFile.Seek(0, 2)
		if err != nil {
			return err
		}*/
		address, err := dataFile.Seek(0, 1)
		if err != nil {
			return err
		}

		_, err = dataFile.Write(newRecord)
		if err != nil {
			return err
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
			return err
		}
		if it%N == 0 {

			summarySize := recordUtil.KEY_SIZE + len(record.Key) + 8
			summaryRecord := make([]byte, summarySize, summarySize)
			binary.LittleEndian.PutUint64(summaryRecord[:], uint64(len(record.Key)))
			for i := 0; i < len(record.Key); i++ {
				summaryRecord[recordUtil.KEY_SIZE+i] = record.Key[i]
			}
			binary.LittleEndian.PutUint64(summaryRecord[recordUtil.KEY_SIZE+len(record.Key):], uint64(indexAddress))
			_, err = summaryFile.Write(summaryRecord)
			if err != nil {
				return err
			}
		}
		it++

		bloom := bloomFilter.NewBloomFilter(0.001, len(pairs))
		bloomBytes := bloom.Encode()
		_, err = filterFile.Write(bloomBytes)
		if err != nil {
			return err
		}

	}
	indexFile.Close()
	dataFile.Close()
	filterFile.Close()
	tocFile.Close()
	summaryFile.Close()
	ss.currentIndex++
	return nil

}
