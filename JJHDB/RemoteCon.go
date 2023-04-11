package JJHDB

import (
	// "net"
	"fmt"
	"net/rpc"
	"sync"
)

type Server struct {
	mutex sync.Mutex

	Client *rpc.Client
	Status int

	ready int //0：unready  1:ready  2:delete

}

const UNREADY int = 0
const READY int = 1
const DELETED int = 2

func NewServer(address string, status int) *Server {
	var server *Server = nil
	server = new(Server)
	server.Status = status
	server.ready = UNREADY
	conn, err1 := rpc.Dial("tcp", address)
	if err1 != nil {
		fmt.Println(err1)
		// fmt.Println("error when dial")
		return nil
	}

	server.Client = conn

	return server
}

func (db *JDB) repSSTable(server *Server) {
	db.addBackworkcnt()

	db.generateSSTable()

	num := make(chan struct{})

	db.sst_mutex.RLock()
	L := len(db.sstlist)
	for i := L - 1; i >= 0; i-- {
		var res ReplyADS
		req := RequestADS{}

		content := db.sstlist[i].readall()
		req.SSTabelid = db.sstlist[i].id
		req.Filebytes = content
		// fmt.Println(content)
		syncCall := server.Client.Go("DBServer.ADDSSTable", req, &res, nil)

		go func(call *rpc.Call, cnt2 chan struct{}) {
			replayDone := <-syncCall.Done
			fmt.Println("同步复制SSTable", replayDone.Error)
			cnt2 <- struct{}{}
		}(syncCall, num)
	}
	db.sst_mutex.RUnlock()

	for i := 0; i < L; i++ {
		<-num
	}
	KVlist := make([]SeqKV, 0)
	db.mem.mutex.RLock()
	for it := db.mem.table.Iterate(); it.IsNotEnd(); it.MoveToNext() {
		KV := SeqKV{}
		KV.seqNumber = it.Key().seqNumber
		KV.key = it.Key().key
		KV.value = it.Value().val

		KVlist = append(KVlist, KV)
	}

	server.mutex.Lock()
	server.ready = READY
	server.mutex.Unlock()

	db.mem.mutex.RUnlock()

	db.delBackworkcnt()

	for i := range KVlist {
		server.Replication(KVlist[i].key, KVlist[i].value, KVlist[i].seqNumber, nil)
	}

}

func (server *Server) Replication(key string, value string, index uint64, cnt chan struct{}) bool {

	server.mutex.Lock()

	if server.ready != READY {
		server.mutex.Unlock()
		return true
	}
	server.mutex.Unlock()

	if server.Status == follower {
		var res Reply
		req := Request{
			Key:   key,
			Value: value,
			Index: index,
		}

		syncCall := server.Client.Go("DBServer.Replication", req, &res, nil)

		go func(call *rpc.Call) {
			// replayDone := <-syncCall.Done
			<-syncCall.Done
			// fmt.Println("异步: ", replayDone)
		}(syncCall)
	} else {
		var res Reply
		req := Request{
			Key:   key,
			Value: value,
			Index: index,
		}

		syncCall := server.Client.Go("DBServer.Replication", req, &res, nil)

		go func(call *rpc.Call, cnt chan struct{}) {
			replayDone := <-syncCall.Done
			// fmt.Println("同步信息发送成功：", replayDone)
			if replayDone.Error != nil && replayDone.Error.Error() == "connection is shut down" {
				server.ready = DELETED
			}
			// fmt.Println(replayDone.Error.Error())
			cnt <- struct{}{}
		}(syncCall, cnt)

	}

	return true
}
