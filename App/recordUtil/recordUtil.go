package recordUtil

import (
	"fmt"
	"hash/crc32"
	"runtime/debug"
)

const (
	CRC_SIZE       = 4
	TIMESTAMP_SIZE = 16
	TOMBSTONE_SIZE = 1
	KEY_SIZE       = 8
	VALUE_SIZE     = 8
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func TryCatch(f func()) func() error {
	return func() (err error) {
		defer func() {
			if panicInfo := recover(); panicInfo != nil {
				err = fmt.Errorf("%v, %s", panicInfo, string(debug.Stack()))
				return
			}
		}()
		f() // calling the decorated function
		return err
	}
}
