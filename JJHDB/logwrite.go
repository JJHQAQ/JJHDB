package JJHDB
import (
	"os"
	"fmt"
	"path/filepath"
	"strconv"
	"encoding/binary"
)

func (db *JDB)newlogfilename() string {
	db.version.logfileid++
	filename:= filepath.Join(db.version.maindir,"log","log-"+strconv.Itoa(db.version.logfileid)+".txt")
	return filename
}

func (db *JDB)newlogfile() *os.File{

	filename:= db.newlogfilename()

	file,err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)

	if err != nil {
        fmt.Println(err)
    }

	return file
}

func writeone(f *os.File,index uint64,p KVpair){

	var num uint32 = 0

	buf8 :=make([]byte,8)
	binary.BigEndian.PutUint64(buf8, index)
	f.Write(buf8)

    b1:=[]byte(p.key)
    num = uint32(len(b1))
	buf :=make([]byte,4)
    binary.BigEndian.PutUint32(buf, num)
    f.Write(buf)
    f.Write(b1)

    b2:=[]byte(p.value.val)
    num = uint32(len(b2))
    binary.BigEndian.PutUint32(buf, num)
    f.Write(buf)
    f.Write(b2)
}

func (db *JDB)logWrite(batch *Batch,ready *[](chan uint64)) {
	 if db.version.logfile == nil {
		db.version.logfile = db.newlogfile()
	 }
	 file:= db.version.logfile
	 for i,p :=range batch.entrys {
		db.version.lastSeq++
		// fmt.Println(db.version.lastSeq,p)
		writeone(file,db.version.lastSeq,p)
		(*ready)[i]<-db.version.lastSeq 
	 }
}