package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"

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
	ck.servers_ = servers
	ck.client_id_ = nrand()
	ck.seq_num_ = 1
	ck.leader_ = 0
	ck.server_num_ = len(servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &QueryArgs{Num: num}
	server := ck.leader_
	for {
		var reply QueryReply
		ok := ck.servers_[server].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.leader_ = server
			return reply.Config
		}
		server = (server + 1) % ck.server_num_
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &JoinArgs{Servers: servers, ClientId: ck.client_id_, SeqNum: ck.seq_num_}
	server := ck.leader_
	for {
		var reply QueryReply
		ok := ck.servers_[server].Call("ShardCtrler.Join", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.seq_num_++
			ck.leader_ = server
			return
		}
		server = (server + 1) % ck.server_num_
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &LeaveArgs{GIDs: gids, ClientId: ck.client_id_, SeqNum: ck.seq_num_}
	server := ck.leader_
	for {
		var reply QueryReply
		ok := ck.servers_[server].Call("ShardCtrler.Leave", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.seq_num_++
			ck.leader_ = server
			return
		}
		server = (server + 1) % ck.server_num_
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &MoveArgs{Shard: shard, GID: gid, ClientId: ck.client_id_, SeqNum: ck.seq_num_}
	server := ck.leader_
	for {
		var reply QueryReply
		ok := ck.servers_[server].Call("ShardCtrler.Move", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.seq_num_++
			ck.leader_ = server
			return
		}
		server = (server + 1) % ck.server_num_
	}
}
