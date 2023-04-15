package JDBClient

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type JClient struct {
	mutex     sync.Mutex
	ServerIPS []string
	LeaderIP  string
	MasterIP  string
}

func Make() *JClient {
	client := JClient{}
	// client.ServerIPS = append(client.ServerIPS, "127.0.0.1:8080")
	client.MasterIP = "127.0.0.1:8079"
	rand.Seed(time.Now().UnixNano())
	client.getAllNode()
	return &client
}

func (client *JClient) randIP() string {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	L := len(client.ServerIPS)
	if L > 0 {
		i := rand.Intn(len(client.ServerIPS))
		return client.ServerIPS[i]
	}
	return ""
}

type RequestGET struct {
	Key   string
	Index uint64
}

type ReplyGET struct {
	Success bool
	Value   string
}

func (client *JClient) Get(key string, index uint64) (bool, string) {
	err, OK, res := client.get(key, index)
	if err != nil {
		client.getAllNode()
		err, OK, res = client.get(key, index)
		if err != nil {
			OK = false
		}
	}
	fmt.Println(res)
	return OK, res
}

func (client *JClient) get(key string, index uint64) (error, bool, string) {

	IP := client.randIP()

	conn, err := rpc.Dial("tcp", IP)
	if err != nil {
		fmt.Println(err)
		return err, false, ""
	}
	defer conn.Close()

	var res ReplyGET
	req := RequestGET{
		Key:   key,
		Index: index,
	}
	err = conn.Call("DBServer.Get", req, &res)

	return err, res.Success, res.Value
}

type RequestPUT struct {
	Key   string
	Value string
}

type ReplyPUT struct {
	Seq uint64
}

func (client *JClient) Put(key string, value string) uint64 {
	seq := client.put(key, value)
	if seq == 0 {
		client.getAllNode()
		seq = client.put(key, value)
	}

	return seq
}

func (client *JClient) put(key string, value string) uint64 {

	client.mutex.Lock()
	IP := client.LeaderIP
	client.mutex.Unlock()

	if len(IP) == 0 {
		return 0
	}

	conn, err := rpc.Dial("tcp", IP)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	defer conn.Close()

	var res ReplyPUT
	req := RequestPUT{
		Key:   key,
		Value: value,
	}
	err = conn.Call("DBServer.Put", req, &res)

	if err != nil {
		log.Fatal(err)
	}

	return res.Seq
}

type RequestClient struct {
}

type ReplyClient struct {
	LeaderIP string
	AllNode  []string
}

func (client *JClient) getAllNode() {
	conn, err := rpc.Dial("tcp", client.MasterIP)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	var res ReplyClient
	req := RequestClient{}
	err = conn.Call("MasterServer.GetAllNode", req, &res)

	if err != nil {
		log.Fatal(err)
	}
	client.mutex.Lock()
	defer client.mutex.Unlock()
	client.LeaderIP = res.LeaderIP
	client.ServerIPS = res.AllNode
	return
}
