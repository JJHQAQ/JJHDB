package JJHDB

import (
	// "sync"
	// "time"
	// "os"
	"fmt"
)

func (db *JDB)searchInmem(key string,index uint64) (bool,Value) {
	flag:=false
	value:=Value{}
	Key:=Internalkey{key:key,seqNumber:index}
	db.mem.mutex.RLock()
	node:=db.mem.table.LowerBound(Key)

	if (node.IsNotEnd() && node.Key().key == key) {
		flag = true
		value = node.Value()
	}
	db.mem.mutex.RUnlock()

	db.imm.mutex.RLock()
	node = db.imm.table.LowerBound(Key)
	if (node.IsNotEnd() && node.Key().key == key) {
		flag = true
		value = node.Value()
	}
	db.imm.mutex.RUnlock()


	return flag,value
}

func (db *JDB)searchInSSTabel(key string,index uint64) (bool,Value) {
	flag:=false
	value:=Value{}
	db.sst_mutex.RLock()
	for i:=len(db.sstlist)-1;i>=0;i-- {
		fmt.Println("start to find in ",db.sstlist[i].pathname)
		ok,val :=db.sstlist[i].find(key,index)
		if (ok) {
			fmt.Println("find in searchInSSTable",val)
			value.val = val
			flag = true
			break
		}
	}
	db.sst_mutex.RUnlock()
	

	return flag,value
}