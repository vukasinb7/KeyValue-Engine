package pair

import "sort"

type byKey []KVPair

func (k byKey) Len() int{
	return len(k)
}
func (k byKey) Less(i, j int) bool{
	return k[i].Key < k[j].Key
}
func (k byKey) Swap(i, j int){
	k[i], k[j] = k[j], k[i]
}

func SortByKey(list []KVPair){
	sort.Sort(byKey(list))
}