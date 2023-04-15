package JJHDB

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

const leader int = 0
const back_up_leader int = 1
const follower int = 2

type Sstablename struct {
	name string
	id   int
}

type SstableNameList []Sstablename

func (list SstableNameList) Len() int           { return len(list) }
func (list SstableNameList) Less(i, j int) bool { return list[i].id < list[j].id }
func (list SstableNameList) Swap(i, j int)      { list[i], list[j] = list[j], list[i] }

type Version struct {
	mutex           sync.Mutex
	LastSeq         uint64
	Logfileid       int
	LogFileName     string
	LastLogFileName string
	Maindir         string

	Tablemax     int
	Sstableid    int
	sstablenames SstableNameList

	Sstablenames []string

	LocalAddress string

	Status   int //0:leader  1:back-up leader  2:follower
	LeaderIP string

	MasterIP    string
	LogServerIP string
}

func (v *Version) AddSstablename(name string, id int) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	v.sstablenames = append(v.sstablenames, Sstablename{name: name, id: id})
	sort.Sort(v.sstablenames)
}

func (v *Version) initversion() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
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

	for i := range v.Sstablenames {
		v.sstablenames = append(v.sstablenames, Sstablename{name: v.Sstablenames[i], id: i + 1})
	}
}

func (v *Version) persist() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	v.Sstablenames = make([]string, 0)
	for i := range v.sstablenames {
		v.Sstablenames = append(v.Sstablenames, v.sstablenames[i].name)
	}

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
