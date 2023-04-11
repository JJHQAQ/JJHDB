package JJHDB

import (
	"fmt"
	"net"
	"net/rpc"
)

type DBServer struct {
	db *JDB
}

type Request struct {
	Key   string
	Value string
	Index uint64
}

type Reply struct {
	Success bool
}

func (this DBServer) Replication(req Request, res *Reply) error {
	if this.db.version.Status == leader {
		res.Success = false
		return nil
	}
	fmt.Println("接收到同步信息：", req)
	res.Success = this.db.putWithIndex(req.Key, req.Value, req.Index)

	return nil
}

type RequestReg struct {
	LocalAddress string
	Status       int
}

type ReplyReg struct {
	Success bool
	// LastSeq       uint64
	// LastSSTableid int
}

func (this DBServer) Register(req RequestReg, res *ReplyReg) error {
	if this.db.version.Status != leader {
		res.Success = false
		return nil
	}

	this.db.register(req.LocalAddress, req.Status)

	// res.LastSeq = this.db.version.LastSeq

	res.Success = true
	return nil
}

type RequestADS struct {
	Filebytes []byte
	SSTabelid int
}

type ReplyADS struct {
	Success bool
}

func (this DBServer) ADDSSTable(req RequestADS, res *ReplyADS) error {

	this.db.addSSTable(req.Filebytes, req.SSTabelid)

	res.Success = true

	return nil
}

type RequestGET struct {
	Key   string
	Index uint64
}

type ReplyGET struct {
	Success bool
	Value   string
}

func (this DBServer) Get(req RequestGET, res *ReplyGET) error {
	res.Success, res.Value = this.db.Get(req.Key, req.Index)
	// fmt.Println(res.Value)
	return nil
}

type RequestPUT struct {
	Key   string
	Value string
}

type ReplyPUT struct {
	seq uint64
}

func (this DBServer) Put(req RequestPUT, res *ReplyPUT) error {
	res.seq = this.db.Put(req.Key, req.Value)
	return nil
}

func (db *JDB) register(address string, status int) {

	S := NewServer(address, status)

	db.servermutex.Lock()
	if status == 1 {
		db.backLeaderList = append(db.backLeaderList, S)
	} else {
		db.followeList = append(db.followeList, S)
	}
	db.servermutex.Unlock()

	go db.repSSTable(S)
}

func (db *JDB) StartService() {
	var DBS *DBServer
	DBS = new(DBServer)
	DBS.db = db
	err1 := rpc.RegisterName("DBServer", DBS)
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	listener, err2 := net.Listen("tcp", db.version.LocalAddress)
	if err2 != nil {
		fmt.Println(err2)
		return
	}
	defer listener.Close()

	db.FindLeader()

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

func (db *JDB) FindLeader() {
	if db.version.Status == leader {
		return
	}

	conn, err1 := rpc.Dial("tcp", db.version.LeaderIP)
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	defer conn.Close()
	var res ReplyReg
	req := RequestReg{
		LocalAddress: db.version.LocalAddress,
		Status:       db.version.Status,
	}
	err2 := conn.Call("DBServer.Register", req, &res)
	if err2 != nil {
		fmt.Println(err2)
	}
	if res.Success {
		fmt.Printf("连接成功")
	} else {
		panic("connect error\n")
	}
	// fmt.Printf("%#v", res)

	// db.syncRep()

}

// func (db *JDB) syncRep() {

// }
