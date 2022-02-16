package memTable

import "pair"

func (memTable *MemTable) Flush() []pair.KVPair {
	// ================
	// Description:
	// ================
	// Returns sorted KVPair array

	data := memTable.list.GetData()
	pair.SortByKey(data)
	return data
}

func (memTable *MemTable) Insert(pair pair.KVPair) {
	isNew := memTable.list.Insert(pair)
	if isNew {
		memTable.size += pair.Size()
	}
}

func (memTable *MemTable) Delete(key string) {
	isNew := memTable.list.Delete(key)
	if isNew {

		memTable.size += uint32(len(key))
	}

}

func (memTable *MemTable) Size() uint32 {
	// ================
	// Description:
	// ================
	// Returns size of MemTable in bytes

	return memTable.size
}

func (memTable *MemTable) Threshold() uint32 {
	// ================
	// Description:
	// ================
	// Returns size of MemTable in bytes

	return memTable.threshold
}
