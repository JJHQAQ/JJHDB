package JJHDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func (db *JDB) newlogfilename() string {
	db.version.Logfileid++
	db.version.persist()
	filename := filepath.Join(db.version.Maindir, "log", "log-"+strconv.Itoa(db.version.Logfileid)+".txt")
	return filename
}

func (db *JDB) newlogfile() *os.File {

	filename := db.newlogfilename()

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)

	if err != nil {
		fmt.Println(err)
	}

	return file
}

func writeone(f *os.File, index uint64, p KVpair) {

	var num uint32 = 0

	buf8 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf8, index)
	f.Write(buf8)

	b1 := []byte(p.key)
	num = uint32(len(b1))
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, num)
	f.Write(buf)
	f.Write(b1)

	b2 := []byte(p.value.val)
	num = uint32(len(b2))
	binary.BigEndian.PutUint32(buf, num)
	f.Write(buf)
	f.Write(b2)
}

func (db *JDB) logWrite(batch *Batch, ready *[](chan uint64)) {
	db.arrangeMem()

	file := db.logfile
	db.mem.mutex.Lock()
	for i, p := range batch.entrys {
		var index uint64 = 0
		if batch.indexs[i] == 0 {
			db.version.LastSeq++
			index = db.version.LastSeq
		} else {
			index = batch.indexs[i]
			if index > db.version.LastSeq {
				db.version.LastSeq = index
			}
		}
		// fmt.Println(db.version.lastSeq,p)
		writeone(file, index, p)
		(*ready)[i] <- index
		db.mem.Put(index, p)
	}
	db.mem.mutex.Unlock()
}
