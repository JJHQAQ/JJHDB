package JJHDB

import(
	"os"
)

type Version struct {
	lastSeq   uint64
	logfile   *os.File
	logfileid int
	maindir   string
}

func (db *JDB)initversion(){
	db.version.lastSeq = 0
	db.version.logfile = nil
	db.version.maindir = "."
}