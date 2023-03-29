package JJHDB

import (
	"sync"
	"time"
	"os"
	// "fmt"
)

type JDB struct {

	mutex		sync.Mutex
	mem 		*Memtable
	imm  		*Memtable
	version 	Version
	logfile     *os.File

	writeToLog	chan Work

	sstlist 	[]*SSTable
	sst_mutex	sync.RWMutex
}



func Make() *JDB {
	db:= JDB{}
	db.mem = newMemtable()
	db.imm = newMemtable()
	db.writeToLog = make(chan Work,1000)
	db.logfile = nil
	db.version.initversion()
	db.recoverFromLog()
	db.recoverSSTable()
	return &db
}

func (db *JDB)Put(key string,value string) uint64{
	w:=BuildWork(key,value)
	db.writeToLog<-w
	seq:=<-w.Done

	return seq
}

func (db *JDB)Get(key string,index uint64) (bool,string) {
	if (index==0) {
		index = db.version.LastSeq
	}

	// for it:=db.mem.table.Iterate();it.IsNotEnd();it.MoveToNext() {
	// 	fmt.Println(it.Key(),it.Value())
	// }

	flag,V:=db.searchInmem(key,index)
	// fmt.Println("pass the mem")
	if (flag) {
		return true,V.val
	}

	flag,V = db.searchInSSTabel(key,index)

	return flag,V.val
}

func (db *JDB)logWriter() {
	
	batch:= Batch{}
	ready:= [](chan uint64){}
	timeout := 100*time.Millisecond
	for {
		select {
		case work:=<-db.writeToLog:
			batch.AppendRaw(work.key,work.val)
			ready = append(ready,work.Done)
			if (batch.size()>=10) {
				db.logWrite(&batch,&ready)
				batch = Batch{}
				ready = [](chan uint64){}
			}
		case <-time.After(timeout):
			if (batch.size()==0){
				continue
			}
			db.logWrite(&batch,&ready)
			batch = Batch{}
			ready = [](chan uint64){}
		}
	}

}

func (db *JDB)compaction() {
	//TODO
}

func (db *JDB)backWork() {
	
	go db.logWriter()
	go db.compaction()
}

func (db *JDB)Start() {


	go db.backWork()

}