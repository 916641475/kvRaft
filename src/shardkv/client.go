package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	controler_clerk_ *shardctrler.Clerk
	config_          shardctrler.Config
	make_end         func(string) *labrpc.ClientEnd
	client_id_       int64
	seq_num_         int
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.controler_clerk_ = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.client_id_ = nrand()
	ck.seq_num_ = 1
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	for {
		shard := key2shard(key)
		gid := ck.config_.Shards[shard]
		if servers, ok := ck.config_.Groups[gid]; ok {
			for server_index := 0; server_index < len(servers); server_index++ {
				server := ck.make_end(servers[server_index])
				var reply Reply
				ok := server.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config_ = ck.controler_clerk_.Query(-1)
	}

}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.ArgsMarker = ArgsMarker{ClientId: ck.client_id_, SeqNum: ck.seq_num_}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := ck.config_.Shards[shard]
		if servers, ok := ck.config_.Groups[gid]; ok {
			for server_index := 0; server_index < len(servers); server_index++ {
				srv := ck.make_end(servers[server_index])
				var reply Reply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.seq_num_++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config_ = ck.controler_clerk_.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
