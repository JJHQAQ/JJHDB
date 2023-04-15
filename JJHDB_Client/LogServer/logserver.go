package LogServer

import (
	"fmt"
	"net"
	"net/rpc"
)

type LogRemote struct {
	Address string
	ch      chan string
}

func Make(ch chan string) *LogRemote {
	lg := LogRemote{}
	lg.ch = ch
	lg.Address = "127.0.0.1:8078"
	return &lg
}

func (lg *LogRemote) Start() {
	var LOGS *LOGServer
	LOGS = new(LOGServer)
	LOGS.ch = lg.ch
	err := rpc.RegisterName("LOGServer", LOGS)
	if err != nil {
		panic(err)
	}
	listener, err1 := net.Listen("tcp", lg.Address)
	if err1 != nil {
		panic(err1)
	}
	defer listener.Close()

	for {
		conn, err3 := listener.Accept()
		fmt.Println("connect from", conn.RemoteAddr().String())
		if err3 != nil {
			fmt.Println(err3)
			return
		}
		go rpc.ServeConn(conn)
	}
}

type LOGServer struct {
	ch chan string
}

type RequestLog struct {
	Message string
}

type ReplyLog struct {
	OK bool
}

func (this LOGServer) Log(req RequestLog, res *ReplyLog) error {
	this.ch <- req.Message
	res.OK = true
	return nil
}
