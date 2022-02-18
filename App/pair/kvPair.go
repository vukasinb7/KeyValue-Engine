package pair

type KVPair struct {
	Key       string
	Value     []byte
	Tombstone byte
	Timestamp uint64
}

func (pair *KVPair) Size() uint32 {
	return uint32(len(pair.Key) + len(pair.Value) + 1)
}
