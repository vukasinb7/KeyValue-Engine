module LSMTree

go 1.17

replace (
	SSTable => ../SSTable
	bloomFilter => ../bloomFilter
	memTable => ../memTable
	merkleTree => ../merkleTree
	pair => ../pair
	recordUtil => ../recordUtil
	skipList => ../skipList
	hyperLogLog => ../hyperLogLog
	countMinSketch => ../countMinSketch
)

require (
	memTable v1.0.0
	SSTable v1.0.0
	recordUtil v1.0.0
	hyperLogLog v1.0.0
	countMinSketch v1.0.0
)

require (
	bloomFilter v1.0.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	merkleTree v1.0.0 // indirect
	pair v1.0.0 // indirect
)
