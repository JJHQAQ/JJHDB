package JJHDB



type Internalkey struct {
	key string
	seqNumber uint64
}

type Value struct {
	val  string
}

type KVpair struct {
	key string
	value Value
}


