package JJHDB

import (
	"sync"
	"time"
	// "os"
	// "fmt"
)

type JDB struct {

	mutex		sync.Mutex
	mem 		*Memtable
	imm  		*Memtable
	version 	Version

	writeToLog	chan Work

}



func Make() *JDB {
	db:= JDB{}
	db.mem = newMemtable()
	db.imm = nil
	db.writeToLog = make(chan Work,1000)
	db.initversion()
	return &db
}

func (db *JDB)Put(key string,value string) uint64{
	w:=BuildWork(key,value)
	db.writeToLog<-w
	seq:=<-w.Done

	return seq
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
			
			continue
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

}

func (db *JDB)backWork() {
	
	go db.logWriter()
	go db.compaction()
}

func (db *JDB)Start() {


	go db.backWork()

}