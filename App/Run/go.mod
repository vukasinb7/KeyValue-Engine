module run

go 1.17

replace (
	LSMTree => ../LSMTree
	SSTable => ../SSTable
	bloomFilter => ../bloomFilter
	configurationManager => ../configurationManager
	countMinSketch => ../countMinSketch
	hyperLogLog => ../hyperLogLog
	lru => ../lru
	memTable => ../memTable
	merkleTree => ../merkleTree
	mmap => ../mmap
	pair => ../pair
	recordUtil => ../recordUtil
	skipList => ../skipList
	tokenBucket => ../tokenBucket
	wal => ../wal
)

require (
	LSMTree v1.0.0
	configurationManager v0.0.0-00010101000000-000000000000
	lru v1.0.0
	memTable v1.0.0
	tokenBucket v1.0.0
	wal v1.0.0

)

require (
	SSTable v1.0.0 // indirect
	countMinSketch v1.0.0 // indirect
	hyperLogLog v1.0.0 // indirect

)

require (
	bloomFilter v1.0.0 // indirect
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v1.3.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.0.0-20211205182925-97ca703d548d // indirect
	merkleTree v1.0.0 // indirect
	mmap v1.0.0 // indirect
	pair v1.0.0 // indirect
	recordUtil v1.0.0 // indirect
	skipList v1.0.0 // indirect

)
