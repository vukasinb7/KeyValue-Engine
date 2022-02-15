module run

go 1.17

replace (
	pair => ../pair
	configMng => ../configurationManager
	memTable => ../memTable
	wal => ../wal
	mmap => ../mmap
	skipList => ../skipList
)
require (
	pair v1.0.0
	configMng v1.0.0
	memTable v1.0.0
	wal v1.0.0
	mmap v1.0.0
	skipList v1.0.0
)