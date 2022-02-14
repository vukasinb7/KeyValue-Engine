package pair

type KVPair struct {
	Key 	string
	Value 	[]byte
}

func (pair *KVPair) Size() uint32{
	return uint32(len(pair.Key) + len(pair.Value))
}