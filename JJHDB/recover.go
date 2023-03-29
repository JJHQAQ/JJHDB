package JJHDB

import (
	// "sync"
	// "time"
	"os"
	"fmt"
	"io"
	"encoding/binary"
)

func (db *JDB)logReadOne(file *os.File) error {

	indexbuf:= make([]byte,8)
	_, err := file.Read(indexbuf)
	if (err == io.EOF) {
		return err
	}
	index := binary.BigEndian.Uint64(indexbuf)

	numbuf:= make([]byte,4)
	_, err = file.Read(numbuf)
	if (err == io.EOF) {
		return err
	}
	num:= binary.BigEndian.Uint32(numbuf)

	keybuf:=make([]byte,num)
	_, err = file.Read(keybuf)
	if (err == io.EOF) {
		return err
	}
	key:=string(keybuf)

	numbuf = make([]byte,4)
	_, err = file.Read(numbuf)
	if (err == io.EOF) {
		return err
	}
	num = binary.BigEndian.Uint32(numbuf)

	valuebuf:=make([]byte,num)
	_, err = file.Read(valuebuf)
	if (err == io.EOF) {
		return err
	}
	value:=string(valuebuf)

	kv:= KVpair{key:key,value:Value{value}}
	db.mem.Put(index,kv)
	if index>db.version.LastSeq {
		db.version.LastSeq = index
	}
	return nil
}

func (db *JDB)recoverFromLog(){
	if (len(db.version.LogFileName)==0) {
		return
	}
	file, err := os.Open(db.version.LogFileName)
	if err != nil {
        panic(err)
    }
	
	for db.logReadOne(file)==nil {
		//
	}
	file.Close()
	file,err = os.OpenFile(db.version.LogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)

	if err != nil {
        fmt.Println(err)
    }
	db.logfile = file

}

func (db *JDB)recoverSSTable(){
	for _,path:=range db.version.Sstablename {
		db.sstlist = append(db.sstlist,NewSStable(path))
	}
}