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
	Seq uint64
}

func (this DBServer) Put(req RequestPUT, res *ReplyPUT) error {
	res.Seq = this.db.Put(req.Key, req.Value)
	return nil
}

type RequestHB struct {
}

type ReplyHB struct {
	OK bool
}

var getHB bool = false

func (this DBServer) HeartBeat(req RequestHB, res *ReplyHB) error {
	if !getHB {
		fmt.Println("get heatBeat!")
		go this.db.SendLog("LeaderNode:" + this.db.version.LocalAddress + " build heartbeat protocol with master")
		getHB = true
	}
	if this.db.version.Status == leader {
		res.OK = true
	} else {
		res.OK = false
	}
	return nil
}

type RequestAssign struct {
}

type ReplyAssign struct {
	OK bool
}

func (this DBServer) Assign(req RequestAssign, res *ReplyAssign) error {
	if this.db.version.Status == back_up_leader {
		this.db.version.Status = leader
		this.db.version.persist()
		res.OK = true
	} else {
		res.OK = false
	}
	return nil
}

type RequestNotice struct {
	NewLeader string
}

type ReplyNotice struct {
	OK bool
}

func (this DBServer) Notice(req RequestNotice, res *ReplyNotice) error {
	this.db.addBackworkcnt()
	defer this.db.delBackworkcnt()
	this.db.version.LeaderIP = req.NewLeader
	this.db.removeall()
	this.db.FindLeader()
	res.OK = true
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

func (db *JDB) registerToMaster() {

	type RequestRegM struct {
		IP     string
		Status int
	}

	type ReplyRegM struct {
		LeaderIP string
		Success  bool
	}

	conn, err1 := rpc.Dial("tcp", db.version.MasterIP)
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	defer conn.Close()

	var res ReplyRegM
	req := RequestRegM{
		IP:     db.version.LocalAddress,
		Status: db.version.Status,
	}
	err2 := conn.Call("MasterServer.Register", req, &res)
	if err2 != nil {
		fmt.Println(err2)
	}
	if res.Success {
		fmt.Printf("注册Master成功")

		db.version.LeaderIP = res.LeaderIP
	} else {
		panic("connect error\n")
	}
}

func (db *JDB) StartService() {
	var DBS *DBServer
	DBS = new(DBServer)
	DBS.db = db
	err1 := rpc.RegisterName("DBServer", DBS)
	if err1 != nil {
		panic(err1)
	}
	listener, err2 := net.Listen("tcp", db.version.LocalAddress)
	if err2 != nil {
		panic(err2)
	}
	defer listener.Close()

	db.registerToMaster()
	db.FindLeader()
	go db.SendLog("Node:" + db.version.LocalAddress + "  Start!")
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
