package JJHDB

import(
	"os"
	"io/ioutil"
	"fmt"
	"encoding/json"
)

type Version struct {
	LastSeq   uint64
	Logfileid int
	LogFileName string
	Maindir   string

	Tablemax  int
	Sstableid int
	Sstablename []string
}

func (v *Version)initversion(){

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

func (v *Version)persist(){
	file, err := ioutil.TempFile(".\\minifest", "minifest-*.txt")
    if err != nil {
        fmt.Println(err)
    }
	json_str, err := json.Marshal(v)
	if err != nil {
        panic(err)
    }
	file.Write(json_str)
	name:=file.Name()
	file.Close()
	os.Remove(".\\minifest\\minifest")
	
	err = os.Rename(name,".\\minifest\\minifest") 
	if err != nil {
        panic(err)
    }
}