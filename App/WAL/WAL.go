package main

import (
	"encoding/binary"
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
	segmentSize     uint8
	segmentIndex    uint8
	currentFile     *os.File
	parentDirectory string
	//treba mapa
}

func createWal(segmentSize uint8, parentDirectory string) Wal {
	segments, err := ioutil.ReadDir(parentDirectory)
	if err != nil {
		log.Fatal(err)
	}

	var currentFile *os.File
	var segmentIndex uint8
	if len(segments) == 0 {
		file, err := os.Create(parentDirectory + "wal_0001.log.bin")
		if err != nil {
			log.Fatal(err)
		}
		currentFile = file
		segmentIndex = 1
	} else {
		file, err := os.Open(parentDirectory + segments[len(segments)-1].Name())
		if err != nil {
			log.Fatal(err)
		}
		currentFile = file
		segmentIndex = uint8(len(segments))
	}

	createdWal := Wal{
		segmentSize:     segmentSize,
		segmentIndex:    segmentIndex,
		currentFile:     currentFile,
		parentDirectory: parentDirectory,
	}

	return createdWal
}

func (wal *Wal) insertRecord(key string, value []byte) {
	recordSize := CRC_SIZE + TOMBSTONE_SIZE + TIMESTAMP_SIZE + KEY_SIZE + VALUE_SIZE + len(key) + len(value)
	newRecord := make([]byte, recordSize, recordSize)

	crc := CRC32(value)
	currentTime := time.Now()
	timestamp := currentTime.Unix()

	binary.LittleEndian.PutUint32(newRecord[:], crc)
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE:], uint64(timestamp))
	newRecord[CRC_SIZE+TIMESTAMP_SIZE] = byte(TOMBSTONE_INSERT)
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE:], uint64(len(key)))
	binary.LittleEndian.PutUint64(newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE:], uint64(len(value)))
	for i := 0; i < len(key); i++ {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE+VALUE_SIZE+i] = key[i]
	}
	for i := 0; i < len(value); i++ {
		newRecord[CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE+VALUE_SIZE+len(key)+i] = value[i]
	}

	f := wal.currentFile
	_, err := f.Write(newRecord)
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	wal := createWal(10, "Data/WAL/")
	wal.insertRecord("asdasd", []byte{1, 2, 3, 4, 5})

	wal.insertRecord("69420", []byte{4, 4, 4, 4, 4, 4})
}

type WalRecord struct {
	crc       uint32
	timestamp uint64
	tombstone bool
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte
}
