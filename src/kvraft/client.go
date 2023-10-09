package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	client_id_  int64
	seq_num_    int
	servers_    []*labrpc.ClientEnd
	leader_     int
	server_num_ int
	mu          sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.client_id_ = nrand()
	ck.seq_num_ = 1
	ck.servers_ = servers
	ck.leader_ = 0
	ck.server_num_ = len(servers)
	return ck
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &GetArgs{Key: key}
	reply := &GetReply{}

	server := ck.leader_
	for {
		end_time := time.Now().Add(kTimeoutThsh)
		var ok bool
		go func(args *GetArgs, reply *GetReply) {
			ok = ck.servers_[server].Call("KVServer.Get", args, reply)
		}(args, reply)
		for time.Now().Before(end_time) && !ok {
			time.Sleep(kCheckInterval)
		}
		if ok && reply.Err == OK {
			ck.leader_ = server
			return reply.Value
		}
		server = (server + 1) % ck.server_num_
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &PutAppendArgs{
		Key: key, Value: value, Op: op,
		ClientId: ck.client_id_, SeqNum: ck.seq_num_}
	reply := &PutAppendReply{}

	server := ck.leader_
	for {
		end_time := time.Now().Add(kTimeoutThsh)
		var ok bool
		go func(args *PutAppendArgs, reply *PutAppendReply) {
			ok = ck.servers_[server].Call("KVServer.PutAppend", args, reply)
		}(args, reply)
		for time.Now().Before(end_time) && !ok {
			time.Sleep(kCheckInterval)
		}
		if ok && reply.Err == OK {
			ck.leader_ = server
			ck.seq_num_++
			return
		}
		server = (server + 1) % ck.server_num_
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
