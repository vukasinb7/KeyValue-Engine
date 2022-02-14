package memTable

import "pair"

func (memTable *MemTable) Flush() []pair.KVPair{
	// ================
	// Description:
	// ================
	// Returns sorted KVPair array

	data := memTable.list.GetData()
	pair.SortByKey(data)
	return data
}

func (memTable *MemTable) Insert(pair pair.KVPair) error{
	err := memTable.list.Insert(pair.Key, pair.Value)
	if err != nil{
		return err
	}
	memTable.size += pair.Size()
	return nil
}

func (memTable *MemTable) Delete(key string) ([]byte, error){
	value, err := memTable.list.Delete(key)
	if err != nil{
		return nil, err
	}
	memTable.size -= uint32(len(key) + len(value))
	return value, nil
}

func (memTable *MemTable) Size() uint32{
	// ================
	// Description:
	// ================
	// Returns size of MemTable in bytes

	return memTable.size
}
