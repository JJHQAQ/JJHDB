package JJHDB

import (
	"sync"
	// "time"
	"encoding/binary"
	"io"
	"os"
	// "fmt"
)

type Keypoint struct {
	key    string
	seq    uint64
	offset int64
}

type SSTable struct {
	pathname string
	file     *os.File

	keypoints []Keypoint
	foot      int64
	mutex     sync.Mutex
}

func NewSStable(path string) *SSTable {

	s := SSTable{}
	s.pathname = path
	s.keypoints = make([]Keypoint, 0)
	s.file = nil
	return &s
}

func (S *SSTable) find(key string, index uint64) (bool, string) {
	S.mutex.Lock()
	defer S.mutex.Unlock()
	S.init()
	target := -1

	// fmt.Println("pass  init ")
	for i, k := range S.keypoints {
		// fmt.Println(k.key,key,k.seq,index)
		if k.key < key || (k.key == key && index >= k.seq) {
			target = i
		} else {
			break
		}
	}
	if target == -1 {
		return false, ""
	}
	Bpoint := S.keypoints[target].offset
	// fmt.Println("pass find target")
	var Epoint int64
	if target == len(S.keypoints)-1 {
		Epoint = S.foot
	} else {
		Epoint = S.keypoints[target+1].offset
	}
	// fmt.Println(Bpoint,Epoint)
	S.file.Seek(Bpoint, 0)
	// fmt.Println("pass Seek")
	// fmt.Println("target: ",key," ",index)
	for Bpoint < Epoint {
		s, k, v, num := readKV(S.file)
		Bpoint += num
		// fmt.Println(k,s,key,index,v)
		if key == k && s <= index {
			// fmt.Println("OK!")
			return true, v
		}
	}

	return false, ""
}

func readKV(file *os.File) (seq uint64, key string, value string, num int64) {
	num = 0

	b1 := make([]byte, 8)
	file.Read(b1)
	seq = binary.BigEndian.Uint64(b1)

	b2 := make([]byte, 4)
	file.Read(b2)
	n := binary.BigEndian.Uint32(b2)

	num += int64(n)

	b3 := make([]byte, n)
	file.Read(b3)
	key = string(b3)

	file.Read(b2)
	n = binary.BigEndian.Uint32(b2)
	b4 := make([]byte, n)
	file.Read(b4)
	value = string(b4)

	num += int64(n)

	return seq, key, value, num + 16
}

func readint(file *os.File) uint64 {
	b := make([]byte, 8)
	file.Read(b)
	res := binary.BigEndian.Uint64(b)
	return res
}

func readK(file *os.File) (index uint64, key string, num uint32) {
	b := make([]byte, 8)
	file.Read(b)
	index = binary.BigEndian.Uint64(b)
	buf := make([]byte, 4)
	file.Read(buf)
	num = binary.BigEndian.Uint32(buf)
	keybuf := make([]byte, num)
	file.Read(keybuf)
	key = string(keybuf)
	return index, key, num + 12
}

func (S *SSTable) init() {
	if S.file == nil {
		var err error
		S.file, err = os.Open(S.pathname)
		if err != nil {
			panic(err)
		}

		Epoint, _ := S.file.Seek(-8, io.SeekEnd)
		S.foot = int64(readint(S.file))

		Bpoint, _ := S.file.Seek(S.foot, io.SeekStart)

		for Bpoint != Epoint {
			offset := int64(readint(S.file))
			index, key, num := readK(S.file)
			Bpoint += int64(num)
			S.keypoints = append(S.keypoints, Keypoint{key: key, seq: index, offset: offset})
		}
	}

	// S.file.Seek(0,io.SeekStart)
}
