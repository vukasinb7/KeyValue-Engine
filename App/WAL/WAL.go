package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
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
//TODO Potencijalna promena  current file iz *os.File u string imena fajla
const (
	CRC_SIZE       = 4
	TIMESTAMP_SIZE = 16
	TOMBSTONE_SIZE = 1
	KEY_SIZE       = 8
	VALUE_SIZE     = 8

	TOMBSTONE_INSERT = 0
	TOMBSTONE_DELETE = 1

	SEGMENT_SIZE = 1 * 1024
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Wal struct {
	segmentSize     uint64
	segmentIndex    uint8
	currentFile     string
	parentDirectory string
	lwm             int32 //broj segmenata koji ostavljamo
	//treba mmapa
}

func createWal(segmentSize uint64, parentDirectory string, lwm int32) Wal {
	segments, err := ioutil.ReadDir(parentDirectory)
	if err != nil {
		log.Fatal(err)
	}

	var currentFile string
	var segmentIndex uint8
	if len(segments) == 0 {
		file, err := os.Create(parentDirectory + "wal_0001.log.bin")
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
		currentFile = file.Name()
		segmentIndex = 1
	} else {
		file, err := os.OpenFile(parentDirectory+segments[len(segments)-1].Name(), os.O_RDWR, 065+1)
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
		currentFile = file.Name()
		segmentIndex = uint8(len(segments))
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

func (wal *Wal) insertRecord(key string, value []byte, status bool) {
	recordSize := CRC_SIZE + TOMBSTONE_SIZE + TIMESTAMP_SIZE + KEY_SIZE + VALUE_SIZE + len(key) + len(value)
	newRecord := make([]byte, recordSize, recordSize)

	crc := CRC32(value)
	currentTime := time.Now()
	timestamp := currentTime.Unix()

	binary.LittleEndian.PutUint32(newRecord[:], crc)
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE:], uint64(timestamp))
	if status {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE] = byte(TOMBSTONE_INSERT)
	} else {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE] = byte(TOMBSTONE_DELETE)
	}
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE:], uint64(len(key)))
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE:], uint64(len(value)))
	for i := 0; i < len(key); i++ {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE+VALUE_SIZE+i] = key[i]
	}
	for i := 0; i < len(value); i++ {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE+VALUE_SIZE+len(key)+i] = value[i]
	}

	f, err := os.OpenFile(wal.currentFile, os.O_RDWR, 065+1)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	_, err1 := f.Seek(0, 2)
	if err1 != nil {
		log.Fatal(err1)
	}

	_, err2 := f.Write(newRecord)
	if err2 != nil {
		log.Fatal(err2)
	}
	fileInfo, err3 := os.Stat(wal.currentFile)
	if err3 != nil {
		log.Fatal(err3)
	}

	if fileInfo.Size() > int64(wal.segmentSize) {
		num := fmt.Sprintf("%04d", wal.segmentIndex+1)
		name := "wal_" + num + ".log.bin"
		file, err := os.Create(wal.parentDirectory + name)
		if err != nil {
			log.Fatal(err)
		}
		file.Close()

		wal.currentFile = file.Name()
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
		err2 := os.Rename(wal.parentDirectory+segments[i].Name(), wal.parentDirectory+name)
		if err2 != nil {
			log.Fatal(err2)
		}
	}
	wal.segmentIndex = uint8(len(segments))
	num := fmt.Sprintf("%04d", len(segments))
	name := "wal_" + num + ".log.bin"
	f, err := os.OpenFile(wal.parentDirectory+name, os.O_RDWR, 065+1)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	wal.currentFile = f.Name()

}

func main() {
	wal := createWal(SEGMENT_SIZE, "Data/WAL/", 5)
	wal.insertRecord("asdasd", []byte{1, 2, 3, 4, 5}, true)
	wal.insertRecord("69420", []byte{4, 4, 4, 4, 4, 4}, true)

	for i := 0; i < 200; i++ {
		wal.insertRecord("69420", []byte{4, 4, 4, 4, 4, 4}, true)
	}
	wal.deleteOldSegments()
	for i := 0; i < 220; i++ {
		wal.insertRecord("69420", []byte{4, 4, 4, 4, 4, 4}, true)
	}

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
