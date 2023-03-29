package JJHDB
import (
	"path/filepath"
	"strconv"
	"encoding/binary"
	"os"
	"fmt"
)
func (db *JDB)tablefull() bool {
	return db.mem.table.Len() > db.version.Tablemax
}

func (db *JDB)newSSTableName() string {
	db.version.Sstableid++
	filename:= filepath.Join(db.version.Maindir,"SSTable","SSTable-"+strconv.Itoa(db.version.Logfileid)+".txt")
	return filename
}

func (db *JDB)newSSTablefile() *os.File {
	filename:= db.newSSTableName()

	file,err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)

	if err != nil {
        fmt.Println(err)
    }

	return file
}


func writeKV(f *os.File,k Internalkey,v Value){
	var num uint32 = 0

	buf8 :=make([]byte,8)
	binary.BigEndian.PutUint64(buf8, k.seqNumber)
	f.Write(buf8)

    b1:=[]byte(k.key)
    num = uint32(len(b1))
	buf :=make([]byte,4)
    binary.BigEndian.PutUint32(buf, num)
    f.Write(buf)
    f.Write(b1)

    b2:=[]byte(v.val)
    num = uint32(len(b2))
    binary.BigEndian.PutUint32(buf, num)
    f.Write(buf)
    f.Write(b2)
}

func writeK(f *os.File,k Internalkey){
	var num uint32 = 0

	buf8 :=make([]byte,8)
	binary.BigEndian.PutUint64(buf8, k.seqNumber)
	f.Write(buf8)

    b1:=[]byte(k.key)
    num = uint32(len(b1))
	buf :=make([]byte,4)
    binary.BigEndian.PutUint32(buf, num)
    f.Write(buf)
    f.Write(b1)
}

func writeInt(f *os.File,n int64){
	buf8 :=make([]byte,8)
	binary.BigEndian.PutUint64(buf8, uint64(n))//INT  not UINT
	f.Write(buf8)
}

func (db *JDB)generateSSTable() bool{
	file:=db.newSSTablefile()
	
	breakpoint := make([]int64,0)
	breakval := make([]Internalkey,0)

	count:=-1
	for it:=db.imm.table.Iterate();it.IsNotEnd();it.MoveToNext() {
		count++
		if (count%10==0) {
			count=0
			offset,_ := file.Seek(0,os.SEEK_CUR)
			breakpoint = append(breakpoint,offset)
			breakval = append(breakval,it.Key())
		}
		writeKV(file,it.Key(),it.Value())
	}

	foot,_:= file.Seek(0,os.SEEK_CUR)

	for i,x:= range breakpoint {
		writeInt(file,x)
		writeK(file,breakval[i])
	}

	writeInt(file,foot)

	db.version.Sstablename = append(db.version.Sstablename,file.Name())

	db.sst_mutex.Lock()
	db.sstlist = append(db.sstlist,NewSStable(file.Name()))
	db.sst_mutex.Unlock()

	file.Close()

	db.imm.mutex.Lock()
	db.imm.table.Clear()
	db.imm.mutex.Unlock()
	return true
}

func (db *JDB)arrangeMem() {

	needPersist := false
	if (db.tablefull()){
		if (db.imm.table.Len()==0) {
			db.mem.mutex.Lock()
			db.imm.mutex.Lock()
			db.mem,db.imm = db.imm,db.mem
			db.mem.mutex.Unlock()
			db.imm.mutex.Unlock()
			db.generateSSTable()

			db.logfile.Close()
			db.logfile = nil
			needPersist = true
		}
	}

	if db.logfile == nil {
		db.logfile = db.newlogfile()
		
		db.version.LogFileName = db.logfile.Name()

		needPersist = true
	}


	if (needPersist) {
		db.version.persist()
	}
}