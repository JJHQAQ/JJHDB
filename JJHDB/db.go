package JJHDB

import (
	"sync"
	"time"
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
	db.version.lastSeq = 0
	db.writeToLog = make(chan Work,1000)
	return &db
}

func (db *JDB)Put(key string,value string) bool{

	w:=BuildWork(key,value)
	db.writeToLog<-w

	// seq:=<-w.Done

	return true
}

func (db *JDB)logWriter() {
	
	batch:= Batch{}
	ready:= [](chan int64){}
	timeout := 10*time.Millisecond
	for {
		select {
		case work:=<-db.writeToLog:
			batch.AppendRaw(work.key,work.val)
			ready = append(ready,work.Done)
			if (batch.size()>=10) {
				go logWrite(&batch,&ready)
				batch = Batch{}
				ready = [](chan int64){}
			}
		case <-time.After(timeout):
			go logWrite(&batch,&ready)
			batch = Batch{}
			ready = [](chan int64){}
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