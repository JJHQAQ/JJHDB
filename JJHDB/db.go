package JJHDB

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type JDB struct {
	mutex       sync.Mutex
	mem         *Memtable
	imm         *Memtable
	version     Version
	logfile     *os.File
	backWorkCnt int

	writeToLog chan Work

	sstlist   SSTableList
	sst_mutex sync.RWMutex

	backLeaderList []*Server
	followeList    []*Server
	servermutex    sync.RWMutex

	generating   sync.Mutex
	generateflag bool
}

func Make() *JDB {
	db := JDB{}
	db.mem = newMemtable()
	db.imm = newMemtable()
	db.writeToLog = make(chan Work, 1000)
	db.logfile = nil
	db.generateflag = false
	db.version.initversion()
	if db.version.Status == leader {
		db.recoverFromLog()
		db.recoverSSTable()
	} else {
		db.removeall()
	}
	return &db
}

func (db *JDB) Put(key string, value string) uint64 {
	if db.version.Status != leader {
		return 0
	}

	seq := db.put(key, value)
	db.SendLog("Leader:" + db.version.LocalAddress + " Start to Replicate...")
	db.servermutex.RLock()
	defer db.servermutex.RUnlock()

	for _, s := range db.followeList {
		s.Replication(key, value, seq, nil)
		db.SendLog("Node:" + s.Address + "Get the Replication")
	}

	num := make(chan struct{})
	for _, s := range db.backLeaderList {
		s.Replication(key, value, seq, num)
		db.SendLog("Node:" + s.Address + "Get the Replication")
	}
	L := len(db.backLeaderList)
	for i := 0; i < L; i++ {
		<-num
	}
	db.SendLog("Leader:" + db.version.LocalAddress + "  Put success")
	return seq
}

func (db *JDB) put(key string, value string) uint64 {

	w := BuildWork(key, value, 0)
	db.writeToLog <- w
	seq := <-w.Done

	return seq
}

func (db *JDB) putWithIndex(key string, value string, index uint64) bool {
	w := BuildWork(key, value, index)
	db.writeToLog <- w
	<-w.Done

	return true
}

func (db *JDB) Get(key string, index uint64) (bool, string) {
	fmt.Println("Start to read (key:", key, ")")
	if index == 0 {
		index = db.version.LastSeq
	}

	// for it:=db.mem.table.Iterate();it.IsNotEnd();it.MoveToNext() {
	// 	fmt.Println(it.Key(),it.Value())
	// }
	fmt.Println("Start to find in memtable")
	flag, V := db.searchInmem(key, index)
	// fmt.Println("pass the mem")
	if flag {
		go db.SendLog("Node:" + db.version.LocalAddress + " Get success")
		return true, V.val
	}
	fmt.Println("Start to find in SSTable")
	flag, V = db.searchInSSTabel(key, index)

	go db.SendLog("Node:" + db.version.LocalAddress + " Get success")
	return flag, V.val
}

func (db *JDB) logWriter() {

	batch := Batch{}
	ready := [](chan uint64){}
	timeout := 100 * time.Millisecond
	for {
		select {
		case work := <-db.writeToLog:
			batch.AppendRaw(work.key, work.val, work.index)
			ready = append(ready, work.Done)
			if batch.size() >= 10 {
				db.logWrite(&batch, &ready)
				batch = Batch{}
				ready = [](chan uint64){}
			}
		case <-time.After(timeout):
			if batch.size() == 0 {
				continue
			}
			db.logWrite(&batch, &ready)
			batch = Batch{}
			ready = [](chan uint64){}
		}
	}

}

func (db *JDB) compaction() {
	//TODO
}

func (db *JDB) backWork() {
	go db.logWriter()
	go db.compaction()
}

func (db *JDB) Start() {

	go db.StartService()

	go db.backWork()

}
