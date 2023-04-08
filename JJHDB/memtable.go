package JJHDB

import (
	"stl4go"
	"sync"
)

type Memtable struct {
	table *stl4go.SkipList[Internalkey, Value]
	mutex sync.RWMutex
}

func newMemtable() *Memtable {
	newtable := Memtable{}
	newtable.table = stl4go.NewSkipListFunc[Internalkey, Value](func(a, b Internalkey) int {
		r := stl4go.OrderedCompare(a.key, b.key)
		if r != 0 {
			return r
		}
		return stl4go.OrderedCompare(b.seqNumber, a.seqNumber)
	})

	return &newtable
}

func (M *Memtable) Put(index uint64, p KVpair) {
	M.table.Insert(Internalkey{seqNumber: index, key: p.key}, p.value)
}
