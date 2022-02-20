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
	bloomFilter v1.0.0
	configurationManager v0.0.0-00010101000000-000000000000
	lru v1.0.0
	memTable v1.0.0
	pair v1.0.0
	recordUtil v1.0.0
	tokenBucket v1.0.0
	wal v1.0.0

)

require (
	SSTable v1.0.0 // indirect
	countMinSketch v1.0.0 // indirect
	hyperLogLog v1.0.0 // indirect

)

require (
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/sys v0.0.0-20211205182925-97ca703d548d // indirect
	merkleTree v1.0.0 // indirect
	mmap v1.0.0 // indirect
	skipList v1.0.0 // indirect

)
