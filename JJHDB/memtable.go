package JJHDB

import (
	"stl4go"
)


type Memtable struct {




	table   *stl4go.SkipList[Internalkey, Value]
}

func newMemtable() *Memtable {
	newtable:= Memtable{}
	newtable.table=stl4go.NewSkipListFunc[Internalkey, Value](func (a,b Internalkey)int {
		r := stl4go.OrderedCompare(a.key, b.key)
		if r != 0 {
			return r
		}
		return stl4go.OrderedCompare(a.seqNumber, b.seqNumber)
	})

	return &newtable
}