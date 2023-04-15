package Master

type MasterServer struct {
	owner *master
}

type RequestRegM struct {
	IP     string
	Status int
}

type ReplyRegM struct {
	LeaderIP string
	Success  bool
}

const leader int = 0
const back_up_leader int = 1
const follower int = 2

func (this MasterServer) Register(req RequestRegM, res *ReplyRegM) error {
	if req.Status == leader {
		this.owner.ChangeLeader(req.IP)
	} else if req.Status == back_up_leader {
		this.owner.AddBackLeader(req.IP)
	} else if req.Status == follower {
		this.owner.AddFollower(req.IP)
	}
	this.owner.mutex.Lock()
	res.LeaderIP = this.owner.LeaderIP
	this.owner.mutex.Unlock()
	res.Success = true
	return nil
}

type RequestClient struct {
}

type ReplyClient struct {
	LeaderIP string
	AllNode  []string
}

func (this MasterServer) GetAllNode(req RequestClient, res *ReplyClient) error {
	this.owner.mutex.Lock()
	defer this.owner.mutex.Unlock()
	res.LeaderIP = this.owner.LeaderIP
	res.AllNode = append(res.AllNode, this.owner.LeaderIP)
	res.AllNode = append(res.AllNode, this.owner.Back_up_leaders...)
	res.AllNode = append(res.AllNode, this.owner.Followers...)
	go func() {
		var LOG string
		LOG = "flash All Node:\n"
		LOG = LOG + "LeaderNode:" + res.LeaderIP + "\n"
		LOG = LOG + "All Node:"
		for _, x := range res.AllNode {
			LOG = LOG + x + ","
		}
		go this.owner.SendLog(LOG)
	}()
	return nil
}
