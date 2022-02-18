package recordUtil

import "hash/crc32"

const (
	CRC_SIZE       = 4
	TIMESTAMP_SIZE = 16
	TOMBSTONE_SIZE = 1
	KEY_SIZE       = 8
	VALUE_SIZE     = 8
	ADDRESS_SIZE   = 8

	TOMBSTONE_INSERT = 0
	TOMBSTONE_DELETE = 1
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
