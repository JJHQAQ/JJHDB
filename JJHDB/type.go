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

type Work struct {
	key string
	val string
	Done chan uint64
}

func BuildWork(k string,v string) Work{
	w:=Work{key:k,val:v}
	w.Done = make(chan uint64)
	return w
}

