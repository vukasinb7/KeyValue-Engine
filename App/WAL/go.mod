module wal

go 1.17

replace (
	mmap => ../mmap
	pair => ../pair
	recordUtil => ../recordUtil
)

require (
	mmap v1.0.0
	pair v1.0.0
	recordUtil v1.0.0

)

require (
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	golang.org/x/sys v0.0.0-20211117180635-dee7805ff2e1 // indirect
)
