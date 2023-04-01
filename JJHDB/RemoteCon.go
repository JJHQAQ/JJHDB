package JJHDB

import (
	// "net"
	"net/rpc"
	"fmt"
)


type Server struct {
	Client *rpc.Client
	Status int
}

func NewServer(address string,status int) *Server{
	var server *Server = nil
	server = new(Server)
	server.Status = status

	conn, err1 := rpc.Dial("tcp", address)
    if err1 != nil {
        fmt.Println(err1)
		fmt.Println("error when dial")
        return nil
    }

	server.Client = conn

	return server
}

func (server *Server)Replication(key string,value string,index uint64,cnt *chan struct{}) bool {

	if (server.Status==follower) {
		var res Reply
		req:= Request{
			Key: key,
			Value: value,
			Index: index,
		}

		syncCall:= server.Client.Go("DBServer.Replication",req,&res,nil)

		go func(call *rpc.Call){
			replayDone:=<-syncCall.Done
			fmt.Println("异步: ",replayDone)
		}(syncCall)
	}else{
		var res Reply
		req:= Request{
			Key: key,
			Value: value,
			Index: index,
		}

		syncCall:= server.Client.Go("DBServer.Replication",req,&res,nil)

		go func(call *rpc.Call,cnt2 *chan struct{}){
			replayDone:=<-syncCall.Done
			fmt.Println("同步: ",replayDone)
			*cnt <- struct{}{}
		}(syncCall,cnt)
		
	}


	return true
}

