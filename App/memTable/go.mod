module memTable

go 1.17

replace (
	skipList => ../skipList
	pair => ../pair
)

require (
	skipList v1.0.0
	pair v1.0.0
)