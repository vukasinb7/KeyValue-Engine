package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"mmap"
	"os"
	"pair"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/
// TODO Potencijalna promena  current file iz *os.File u string imena fajla
// TODO Koristiti mmap za pristup disku
const (
	CRC_SIZE       = 4
	TIMESTAMP_SIZE = 16
	TOMBSTONE_SIZE = 1
	KEY_SIZE       = 8
	VALUE_SIZE     = 8

	TOMBSTONE_INSERT = 0
	TOMBSTONE_DELETE = 1
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Wal struct {
	segmentSize     uint64   // size of segment ib bytes
	segmentIndex    uint32   // index of last segment
	currentFile     *os.File // path of currently active segment
	parentDirectory string   // path of directory where segments are located
	lwm             uint32   // number of most recent segments that are not deleted

}

func CreateWal(segmentSize uint64, parentDirectory string, lwm uint32) Wal {
	segments, err := ioutil.ReadDir(parentDirectory)
	if err != nil {
		log.Fatal(err)
	}

	var currentFile *os.File
	var segmentIndex uint32
	if len(segments) == 0 {
		file, err := os.Create(parentDirectory + "wal_0001.log.bin")
		if err != nil {
			log.Fatal(err)
		}
		currentFile = file
		segmentIndex = 1
	} else {
		file, err := os.OpenFile(parentDirectory+segments[len(segments)-1].Name(), os.O_RDWR, 065+1)
		if err != nil {
			log.Fatal(err)
		}
		currentFile = file
		segmentIndex = uint32(len(segments))
	}

	createdWal := Wal{
		segmentSize:     segmentSize,
		segmentIndex:    segmentIndex,
		currentFile:     currentFile,
		parentDirectory: parentDirectory,
		lwm:             lwm,
	}

	return createdWal
}

func (wal *Wal) pushRecord(kvPair pair.KVPair, status bool) {
	recordSize := CRC_SIZE + TOMBSTONE_SIZE + TIMESTAMP_SIZE + KEY_SIZE + VALUE_SIZE + len(kvPair.Key) + len(kvPair.Value)
	newRecord := make([]byte, recordSize, recordSize)

	crc := CRC32(kvPair.Value)
	currentTime := time.Now()
	timestamp := currentTime.Unix()

	binary.LittleEndian.PutUint32(newRecord[:], crc)
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE:], uint64(timestamp))
	if status {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE] = byte(TOMBSTONE_INSERT)
	} else {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE] = byte(TOMBSTONE_DELETE)
	}
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE:], uint64(len(kvPair.Key)))
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE:], uint64(len(kvPair.Value)))
	for i := 0; i < len(kvPair.Key); i++ {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE+VALUE_SIZE+i] = kvPair.Key[i]
	}
	for i := 0; i < len(kvPair.Value); i++ {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE+VALUE_SIZE+len(kvPair.Key)+i] = kvPair.Value[i]
	}

	/*f, err := os.OpenFile(wal.currentFile.Name(), os.O_RDWR, 065+1)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()*/
	_, err1 := wal.currentFile.Seek(0, 2)
	if err1 != nil {
		log.Fatal(err1)
	}

	err2 := mmap.Append(wal.currentFile, newRecord)
	if err2 != nil {
		log.Fatal(err2)
	}
	fileInfo, err3 := os.Stat(wal.currentFile.Name())
	if err3 != nil {
		log.Fatal(err3)
	}

	if fileInfo.Size() > int64(wal.segmentSize) {
		err4 := wal.currentFile.Close()
		if err4 != nil {
			log.Fatal(err3)
		}
		num := fmt.Sprintf("%04d", wal.segmentIndex+1)
		name := "wal_" + num + ".log.bin"
		file, err := os.Create(wal.parentDirectory + name)
		if err != nil {
			log.Fatal(err)
		}
		//file.Close()

		wal.currentFile = file
		wal.segmentIndex++
	}
}

func (wal *Wal) deleteOldSegments() {
	segments, err := ioutil.ReadDir(wal.parentDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(segments)-int(wal.lwm); i++ {
		err := os.Remove(wal.parentDirectory + segments[i].Name())
		if err != nil {
			log.Fatal(err)
		}
	}

	segments, err = ioutil.ReadDir(wal.parentDirectory)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(segments); i++ {
		num := fmt.Sprintf("%04d", i+1)
		name := "wal_" + num + ".log.bin"
		err3 := wal.currentFile.Close()
		if err3 != nil {
			log.Fatal(err3)
		}
		err2 := os.Rename(wal.parentDirectory+segments[i].Name(), wal.parentDirectory+name)
		if err2 != nil {
			log.Fatal(err2)
		}
	}
	wal.segmentIndex = uint32(len(segments))
	num := fmt.Sprintf("%04d", len(segments))
	name := "wal_" + num + ".log.bin"
	f, err := os.OpenFile(wal.parentDirectory+name, os.O_RDWR, 065+1)
	if err != nil {
		log.Fatal(err)
	}
	//defer f.Close()
	wal.currentFile = f

}

/*
type WalRecord struct {
	crc       uint32
	timestamp uint64
	tombstone bool
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte
}*/
