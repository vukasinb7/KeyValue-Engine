module run

go 1.17

replace (
	configMng => ../configurationManager
	lru => ../lru
	memTable => ../memTable
	mmap => ../mmap
	pair => ../pair
	skipList => ../skipList
	wal => ../wal
)

require (
	configMng v1.0.0
	memTable v1.0.0
	wal v1.0.0
)

require (
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	golang.org/x/sys v0.0.0-20211117180635-dee7805ff2e1 // indirect
	mmap v1.0.0 // indirect
	pair v1.0.0 // indirect
	skipList v1.0.0 // indirect
)
