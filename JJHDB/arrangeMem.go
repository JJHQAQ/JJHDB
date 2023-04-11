package JJHDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func (db *JDB) needarrange() bool {
	flag := true

	if db.mem.table.Len() <= db.version.Tablemax {
		flag = false

		return flag
	}
	// fmt.Println("table full")
	if !db.imm.table.IsEmpty() {
		flag = false
		// fmt.Println("imm full")
		return flag
	}

	if db.backWorkCnt > 0 {
		flag = false
		// fmt.Println("backWork running")
	}

	db.generating.Lock()
	if db.generateflag {
		flag = false
	}
	db.generating.Unlock()

	return flag
}

func (db *JDB) newSSTableName(id int) string {

	db.mutex.Lock()
	defer db.mutex.Unlock()
	if id == -1 {
		db.version.Sstableid++

		id = db.version.Sstableid
	} else {
		if id > db.version.Sstableid {
			db.version.Sstableid = id
		}
	}
	filename := filepath.Join(db.version.Maindir, "SSTable", "SSTable-"+strconv.Itoa(id)+".txt")
	return filename
}

func (db *JDB) newSSTablefile(id int) *os.File {
	filename := db.newSSTableName(id)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)

	if err != nil {
		fmt.Println(err)
	}

	return file
}

func writeKV(f *os.File, k Internalkey, v Value) {
	var num uint32 = 0

	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, k.seqNumber)
	f.Write(buf8)

	b1 := []byte(k.key)
	num = uint32(len(b1))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, num)
	f.Write(buf)
	f.Write(b1)

	b2 := []byte(v.val)
	num = uint32(len(b2))
	binary.BigEndian.PutUint32(buf, num)
	f.Write(buf)
	f.Write(b2)
}

func writeK(f *os.File, k Internalkey) {
	var num uint32 = 0

	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, k.seqNumber)
	f.Write(buf8)

	b1 := []byte(k.key)
	num = uint32(len(b1))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, num)
	f.Write(buf)
	f.Write(b1)
}

func writeInt(f *os.File, n int64) {
	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, uint64(n)) //INT  not UINT
	f.Write(buf8)
}

func (db *JDB) generateSSTable() bool {

	db.imm.mutex.Lock()
	db.generating.Lock()
	if db.imm.table.IsEmpty() || db.generateflag {
		fmt.Println("??? Why??: ", db.imm.table.IsEmpty(), db.generateflag)

		db.imm.mutex.Unlock()
		db.generating.Unlock()
		return true
	}
	db.generateflag = true
	db.generating.Unlock()
	db.imm.mutex.Unlock()

	file := db.newSSTablefile(-1)

	breakpoint := make([]int64, 0)
	breakval := make([]Internalkey, 0)

	count := -1
	for it := db.imm.table.Iterate(); it.IsNotEnd(); it.MoveToNext() {
		count++
		if count%10 == 0 {
			count = 0
			offset, _ := file.Seek(0, os.SEEK_CUR)
			breakpoint = append(breakpoint, offset)
			breakval = append(breakval, it.Key())
		}
		writeKV(file, it.Key(), it.Value())
	}

	foot, _ := file.Seek(0, os.SEEK_CUR)

	for i, x := range breakpoint {
		writeInt(file, x)
		writeK(file, breakval[i])
	}

	writeInt(file, foot)

	db.version.AddSstablename(file.Name(), db.version.Sstableid)

	db.sst_mutex.Lock()
	db.sstlist.AddNewSSTable(file.Name(), db.version.Sstableid)
	db.sst_mutex.Unlock()

	file.Close()

	db.version.LastLogFileName = ""
	fmt.Println("Clear this")
	db.version.persist()

	db.imm.mutex.Lock()
	db.imm.table.Clear()
	db.imm.mutex.Unlock()

	db.generating.Lock()
	db.generateflag = false
	db.generating.Unlock()

	return true
}

func (db *JDB) arrangeMem() {

	needPersist := false
	if db.needarrange() {
		db.mem.mutex.Lock()
		db.imm.mutex.Lock()
		db.mem, db.imm = db.imm, db.mem
		db.mem.mutex.Unlock()
		db.imm.mutex.Unlock()
		db.version.LastLogFileName = db.logfile.Name()
		go db.generateSSTable()
		db.logfile.Close()
		db.logfile = nil
		needPersist = true
	}

	if db.logfile == nil {
		db.logfile = db.newlogfile()

		db.version.LogFileName = db.logfile.Name()

		needPersist = true
	}

	if needPersist {
		fmt.Println("persist this")
		db.version.persist()
	}
}
