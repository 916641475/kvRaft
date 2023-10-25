package shardctrler

import (
	"math"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu_      sync.Mutex
	me_      int
	rf_      *raft.Raft
	applyCh_ chan raft.ApplyMsg

	configs_    []Config // indexed by config num
	done_       map[int]chan int
	client_seq_ map[int64]int
}

func (sc *ShardCtrler) ConfigsTail() *Config {
	return &sc.configs_[len(sc.configs_)-1]
}

func (sc *ShardCtrler) NewConfig() Config {
	var conf Config
	conf.Groups = make(map[int][]string)
	conf.Num = sc.ConfigsTail().Num + 1
	for k, v := range sc.ConfigsTail().Groups {
		conf.Groups[k] = v
	}
	for i, s := range sc.ConfigsTail().Shards {
		conf.Shards[i] = s
	}
	return conf
}

func (sc *ShardCtrler) Rebalance(conf *Config) {
	group2shard := map[int][]int{0: {}}
	var gids []int
	for gid := range conf.Groups {
		group2shard[gid] = []int{}
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	for i, gid := range conf.Shards {
		group2shard[gid] = append(group2shard[gid], i)
	}
	for max, max_gid, min, min_gid := MaxMin(group2shard, gids); max-min > 1; max, max_gid, min, min_gid = MaxMin(group2shard, gids) {
		rebalanced_num := (max - min) / 2
		rebalanced_shard := group2shard[max_gid][0:rebalanced_num]
		group2shard[max_gid] = group2shard[max_gid][rebalanced_num:]
		group2shard[min_gid] = append(group2shard[min_gid], rebalanced_shard...)
		for _, t := range rebalanced_shard {
			conf.Shards[t] = min_gid
		}
	}
}

func MaxMin(group2shard map[int][]int, gids []int) (max int, max_gid int, min int, min_gid int) {
	max = 0
	min = math.MaxInt
	for _, gid := range gids {
		l := len(group2shard[gid])
		if l > max {
			max = l
			max_gid = gid
		}
		if l < min {
			min = l
			min_gid = gid
		}
	}
	if len(group2shard[0]) > 0 {
		if min == math.MaxInt {
			return 0, 0, 0, 0
		} else {
			return len(group2shard[0]) * 2, 0, 0, min_gid
		}
	} else {
		return max, max_gid, min, min_gid
	}
}

func (sc *ShardCtrler) DoApply() {
	for v := range sc.applyCh_ {
		if v.CommandValid {
			sc.apply(v)
			if _, isLeader := sc.rf_.GetState(); !isLeader {
				continue
			}
			sc.mu_.Lock()
			ch := sc.done_[v.CommandIndex]
			config_num := -1
			switch cmd := v.Command.(type) {
			case QueryArgs:
				config_num = cmd.Num
				if config_num < 0 || config_num > sc.ConfigsTail().Num {
					config_num = sc.ConfigsTail().Num
				}
			}
			sc.mu_.Unlock()
			go func() {
				ch <- config_num
			}()
		}
	}
}

func (sc *ShardCtrler) apply(v raft.ApplyMsg) {
	switch cmd := v.Command.(type) {
	case JoinArgs:
		if cmd.SeqNum > sc.client_seq_[cmd.ClientId] {
			sc.client_seq_[cmd.ClientId] = cmd.SeqNum
			conf := sc.NewConfig()
			for k, v := range cmd.Servers {
				conf.Groups[k] = v
			}
			sc.Rebalance(&conf)
			sc.mu_.Lock()
			sc.configs_ = append(sc.configs_, conf)
			sc.mu_.Unlock()
		}
	case LeaveArgs:
		if cmd.SeqNum > sc.client_seq_[cmd.ClientId] {
			sc.client_seq_[cmd.ClientId] = cmd.SeqNum
			conf := sc.NewConfig()
			for _, k := range cmd.GIDs {
				for i, gid := range conf.Shards {
					if gid == k {
						conf.Shards[i] = 0
					}
				}
				delete(conf.Groups, k)
			}
			sc.Rebalance(&conf)
			sc.mu_.Lock()
			sc.configs_ = append(sc.configs_, conf)
			sc.mu_.Unlock()
		}
	case MoveArgs:
		if cmd.SeqNum > sc.client_seq_[cmd.ClientId] {
			sc.client_seq_[cmd.ClientId] = cmd.SeqNum
			conf := sc.NewConfig()
			conf.Shards[cmd.Shard] = cmd.GID
			sc.mu_.Lock()
			sc.configs_ = append(sc.configs_, conf)
			sc.mu_.Unlock()
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	_, reply.WrongLeader, reply.Err = sc.Command("Join", *args)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, reply.WrongLeader, reply.Err = sc.Command("Leave", *args)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, reply.WrongLeader, reply.Err = sc.Command("Move", *args)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	num := args.Num
	if _, isLeader := sc.rf_.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	if num < 0 || num > sc.ConfigsTail().Num {
		num = sc.ConfigsTail().Num
	}
	_, reply.WrongLeader, reply.Err = sc.Command("Query", *args)
	sc.mu_.Lock()
	reply.Config = sc.configs_[num]
	sc.mu_.Unlock()
}

const TimeoutInterval = 500 * time.Millisecond

func (sc *ShardCtrler) Command(ty string, args interface{}) (i int, wrongLeader bool, err Err) {
	i, _, isLeader := sc.rf_.Start(args)
	if !isLeader {
		return -1, true, OK
	}
	ch := make(chan int, 1)
	sc.mu_.Lock()
	sc.done_[i] = ch
	sc.mu_.Unlock()
	select {
	case v := <-ch:
		sc.mu_.Lock()
		delete(sc.done_, i)
		sc.mu_.Unlock()
		return v, false, OK
	case <-time.After(TimeoutInterval):
		sc.mu_.Lock()
		delete(sc.done_, i)
		sc.mu_.Unlock()
		return -1, false, ErrTimeout
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf_.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf_
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me_ is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me_ int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me_ = me_

	sc.configs_ = make([]Config, 1)
	sc.configs_[0].Groups = map[int][]string{}

	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh_ = make(chan raft.ApplyMsg)
	sc.rf_ = raft.Make(servers, me_, persister, sc.applyCh_)
	sc.done_ = make(map[int]chan int)
	sc.client_seq_ = make(map[int64]int)
	go sc.DoApply()

	return sc
}
