package JJHDB

type Batch struct {

	entrys []KVpair
	
}

func BuildBatch() Batch {
	return Batch{}
}

func (b Batch)AppendRaw(k string,val string) bool{
	V:=Value{}
	V.val = val
	kv:= KVpair{key:k,value:V}
	b.entrys = append(b.entrys,kv)
	return true
}

func (b Batch)AppendKV(kv KVpair) bool {
	b.entrys = append(b.entrys,kv)
	return true
}
func (b Batch)size()int{
	return len(b.entrys)
}