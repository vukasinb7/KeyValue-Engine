module SSTable

go 1.17

replace (
	bloomFilter => ../bloomFilter
	pair => ../pair
	recordUtil => ../recordUtil
)

require (
	bloomFilter v1.0.0
	pair v1.0.0
	recordUtil v1.0.0
)

require github.com/spaolacci/murmur3 v1.1.0 // indirect
