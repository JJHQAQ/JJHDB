package Master

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type master struct {
	mutex           sync.Mutex
	Address         string
	LeaderIP        string
	Followers       []string
	Back_up_leaders []string

	LogServerIP string

	changeleader chan string

	heartbeat chan int
}

func Make() *master {
	M := master{}
	M.Address = "127.0.0.1:8079"
	M.LogServerIP = "127.0.0.1:8078"
	M.LeaderIP = ""
	M.Back_up_leaders = []string{}
	M.Followers = []string{}
	M.heartbeat = make(chan int)
	return &M
}

func (M *master) Start() {
	var MS *MasterServer
	MS = new(MasterServer)
	MS.owner = M
	err1 := rpc.RegisterName("MasterServer", MS)
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	listener, err2 := net.Listen("tcp", M.Address)
	if err2 != nil {
		fmt.Println(err2)
		return
	}
	defer listener.Close()

	go M.HeartBeat()
	go M.SendLog("Master Start!")
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

func (M *master) ChangeLeader(IP string) {
	M.mutex.Lock()
	M.LeaderIP = IP
	M.mutex.Unlock()

	M.StartNewHeartBeat()
}

func (M *master) AddFollower(IP string) {
	M.mutex.Lock()
	defer M.mutex.Unlock()

	M.Followers = append(M.Followers, IP)
}

func (M *master) AddBackLeader(IP string) {
	M.mutex.Lock()
	defer M.mutex.Unlock()

	M.Back_up_leaders = append(M.Back_up_leaders, IP)
}

func (M *master) HeartBeat() {

	interval := time.Second * 3

	Timer := time.NewTimer(interval)
	for {
		select {
		case <-Timer.C:
			fmt.Println("timeout!")
			M.mutex.Lock()
			if M.LeaderIP == "" {
				M.mutex.Unlock()
				Timer.Reset(interval)
			} else {
				M.mutex.Unlock()
				M.SelectNewLeader()
				M.StartNewHeartBeat()
				fmt.Printf("here")
				Timer.Reset(interval * 2)
			}
		case <-M.heartbeat:
			fmt.Println("get heartbeat!")
			Timer.Reset(interval)
		}
	}

}
func (M *master) SelectNewLeader() {

	type RequestAssign struct {
	}

	type ReplyAssign struct {
		OK bool
	}

	type RequestNotice struct {
		NewLeader string
	}

	type ReplyNotice struct {
		OK bool
	}

	M.mutex.Lock()
	defer M.mutex.Unlock()

	if len(M.Back_up_leaders) == 0 {
		M.SendLog("Lost all stable node, run over")
		panic("Lost all stable node")
	}
	flag := false
	for len(M.Back_up_leaders) != 0 {
		M.LeaderIP = M.Back_up_leaders[len(M.Back_up_leaders)-1]
		M.Back_up_leaders = M.Back_up_leaders[:len(M.Back_up_leaders)-1]

		conn, err := rpc.Dial("tcp", M.LeaderIP)
		if err != nil {
			// fmt.Println(err)
			// fmt.Println("error when dial")
			conn.Close()
			continue
		}
		var res ReplyAssign
		req := RequestAssign{}
		err = conn.Call("DBServer.Assign", req, &res)
		if err != nil || !res.OK {
			conn.Close()
			continue
		} else {
			conn.Close()
			flag = true
			break
		}
	}

	if len(M.Back_up_leaders) == 0 && !flag {
		M.SendLog("Lost all stable node, run over")
		panic("Lost all stable node")
	}
	M.SendLog("Assign New Leader: " + M.LeaderIP)

	for i := range M.Back_up_leaders {

		conn, err := rpc.Dial("tcp", M.Back_up_leaders[i])
		if err != nil {
			conn.Close()
			continue
		}
		var res ReplyNotice
		req := RequestNotice{
			NewLeader: M.LeaderIP,
		}
		conn.Go("DBServer.Notice", req, &res, nil)
		conn.Close()
	}

	for i := range M.Followers {

		conn, err := rpc.Dial("tcp", M.Followers[i])
		if err != nil {
			conn.Close()
			continue
		}
		var res ReplyNotice
		req := RequestNotice{
			NewLeader: M.LeaderIP,
		}
		conn.Go("DBServer.Notice", req, &res, nil)
		conn.Close()
	}

	M.SendLog("All Node Follow The New Leader")
}

func (M *master) StartNewHeartBeat() {

	go func(IP string, HB chan int) {
		fmt.Printf(IP)
		conn, err := rpc.Dial("tcp", IP)
		defer conn.Close()
		if err != nil {
			fmt.Println(err)
			// fmt.Println("error when dial")
			return
		}
		type RequestHB struct {
		}

		type ReplyHB struct {
			OK bool
		}
		var res ReplyHB
		req := RequestHB{}
		for {
			err = conn.Call("DBServer.HeartBeat", req, &res)
			if err != nil || !res.OK {
				fmt.Println(err)
				break
			} else {
				fmt.Println("发送 heatbeat 成功")
				HB <- 1
				time.Sleep(time.Second)
			}
		}
	}(M.LeaderIP, M.heartbeat)
}

func (M *master) SendLog(msg string) {
	type RequestLog struct {
		Message string
	}

	type ReplyLog struct {
		OK bool
	}
	conn, err1 := rpc.Dial("tcp", M.LogServerIP)
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	defer conn.Close()

	var res ReplyLog
	req := RequestLog{
		Message: msg,
	}
	err2 := conn.Call("LOGServer.Log", req, &res)
	if err2 != nil {
		fmt.Println(err2)
	}
	if res.OK {
		fmt.Printf("LOG 发送成功")
	} else {
		panic("connect error\n")
	}
}
