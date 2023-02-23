package JJHDB



type Internalkey struct {
	key string
	seqNumber int64
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
	Done chan int64
}

func BuildWork(k string,v string) Work{
	w:=Work{key:k,val:v}
	w.Done = make(chan int64)
	return w
}

type Version struct {
	lastSeq   int64
}