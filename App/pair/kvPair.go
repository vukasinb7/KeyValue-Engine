package pair

type KVPair struct {
	Key       string `json:"Key"`
	Value     []byte `json:"Value"`
	Tombstone byte
	Timestamp uint64
}

func (pair *KVPair) Size() uint32 {
	return uint32(len(pair.Key) + len(pair.Value) + 1)
}
