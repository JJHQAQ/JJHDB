package JJHDB

import (
	"io/ioutil"
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
	db.removeDir(filepath.Join(db.version.Maindir, "SSTable"))
	db.removeDir(filepath.Join(db.version.Maindir, "log"))
	db.version.LastSeq = 1
	db.version.Sstablename = []string{}
	db.version.LogFileName = ""
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
