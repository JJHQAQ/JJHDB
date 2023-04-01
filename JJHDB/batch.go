package JJHDB

// import "fmt"

type Batch struct {

	entrys []KVpair
	indexs []uint64
}

func BuildBatch() Batch {
	return Batch{}
}

func (b *Batch)AppendRaw(k string,val string,in uint64) bool{
	V:=Value{}
	V.val = val
	kv:= KVpair{key:k,value:V}
	b.entrys = append(b.entrys,kv)
	b.indexs = append(b.indexs,in)
	return true
}

func (b *Batch)AppendKV(kv KVpair) bool {
	b.entrys = append(b.entrys,kv)
	return true
}
func (b Batch)size()int{
	return len(b.entrys)
}

type Work struct {
	index uint64
	key string
	val string
	Done chan uint64
}

func BuildWork(k string,v string,in uint64) Work{
	w:=Work{key:k,val:v,index:in}
	w.Done = make(chan uint64)
	return w
}