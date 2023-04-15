package JJHDB

import (
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
)

func (db *JDB) addBackworkcnt() {
	db.mutex.Lock()
	db.backWorkCnt++
	db.mutex.Unlock()
}

func (db *JDB) delBackworkcnt() {
	db.mutex.Lock()
	db.backWorkCnt--
	db.mutex.Unlock()
}

func (db *JDB) removeall() {
	if db.logfile != nil {
		db.logfile.Close()
		db.logfile = nil
	}
	db.sst_mutex.Lock()
	for i := len(db.sstlist) - 1; i >= 0; i-- {
		db.sstlist[i].Clear()
	}
	db.sstlist.Clear()
	db.sst_mutex.Unlock()

	db.removeDir(filepath.Join(db.version.Maindir, "SSTable"))
	db.removeDir(filepath.Join(db.version.Maindir, "log"))
	db.version.LastSeq = 1
	db.version.Sstablenames = []string{}
	db.version.sstablenames = []Sstablename{}
	db.version.LogFileName = ""
	db.version.LastLogFileName = ""
	db.version.Logfileid = 0
	db.version.Sstableid = 0
	db.version.persist()
}

func (db *JDB) removeDir(deletePath string) error {
	files, err := ioutil.ReadDir(deletePath)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		err = os.RemoveAll(filepath.Join(deletePath, file.Name()))
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (db *JDB) SendLog(msg string) {
	type RequestLog struct {
		Message string
	}

	type ReplyLog struct {
		OK bool
	}
	conn, err1 := rpc.Dial("tcp", db.version.LogServerIP)
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	defer conn.Close()

	var res ReplyLog
	req := RequestLog{
		Message: msg,
	}
	err2 := conn.Call("LOGServer.Log", req, &res)
	if err2 != nil {
		fmt.Println(err2)
	}
	if res.OK {
		fmt.Printf("LOG 发送成功")
	} else {
		panic("connect error\n")
	}
}
