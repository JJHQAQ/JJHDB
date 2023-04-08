package JJHDB

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

const leader int = 0
const back_up_leader int = 1
const follower int = 2

type Version struct {
	LastSeq         uint64
	Logfileid       int
	LogFileName     string
	LastLogFileName string
	Maindir         string

	Tablemax    int
	Sstableid   int
	Sstablename []string

	LocalAddress string

	Status   int //0:leader  1:back-up leader  2:follower
	LeaderIP string
}

func (v *Version) initversion() {

	// v.LastSeq = 0
	// v.Maindir = "."
	// v.Sstableid = 0
	// v.Sstablename = make([]string,0)

	content, err := ioutil.ReadFile("./minifest/minifest")
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(content, v)

	if err != nil {
		fmt.Println("json unmarshal failed!")
		return
	}
}

func (v *Version) persist() {
	file, err := ioutil.TempFile(".\\minifest", "minifest-*.txt")
	if err != nil {
		fmt.Println(err)
	}
	json_str, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		panic(err)
	}
	file.Write(json_str)
	name := file.Name()
	file.Close()
	os.Remove(".\\minifest\\minifest")

	err = os.Rename(name, ".\\minifest\\minifest")
	if err != nil {
		panic(err)
	}
}
