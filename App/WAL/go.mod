module WAL

go 1.17

replace (
	mmap => ../mmap
	pair => ../pair
)

require (
	mmap v1.0.0
	pair v1.0.0
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	golang.org/x/sys v0.0.0-20211117180635-dee7805ff2e1 // indirect
)