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

	ready int //0：unready 1:replicating 2:ready

	Cache []SeqKV
}

const unready int = 0
const replicating int = 1
const ready int = 2

func NewServer(address string, status int) *Server {
	var server *Server = nil
	server = new(Server)
	server.Status = status
	server.ready = unready
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

}

func (server *Server) Replication(key string, value string, index uint64, cnt *chan struct{}) bool {

	if server.ready == unready {
		return true
	}

	if server.ready == replicating {
		server.mutex.Lock()
		defer server.mutex.Unlock()

		server.Cache = append(server.Cache, SeqKV{seqNumber: index, key: key, value: value})

		return true
	}

	if server.Status == follower {
		var res Reply
		req := Request{
			Key:   key,
			Value: value,
			Index: index,
		}

		syncCall := server.Client.Go("DBServer.Replication", req, &res, nil)

		go func(call *rpc.Call) {
			replayDone := <-syncCall.Done
			fmt.Println("异步: ", replayDone)
		}(syncCall)
	} else {
		var res Reply
		req := Request{
			Key:   key,
			Value: value,
			Index: index,
		}

		syncCall := server.Client.Go("DBServer.Replication", req, &res, nil)

		go func(call *rpc.Call, cnt2 *chan struct{}) {
			replayDone := <-syncCall.Done
			fmt.Println("同步信息发送成功：", replayDone)
			*cnt <- struct{}{}
		}(syncCall, cnt)

	}

	return true
}
