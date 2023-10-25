package shardkv

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Config struct {
	Conf      shardctrler.Config
	Committer int
}

type ShardKV struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	mck           *shardctrler.Clerk
	config_       Config
	last_config_  Config
	groups_       map[int][]string
	applyCh       chan raft.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxraftstate_ int // snapshot if log grows this big
	serversLen    int

	shard_states_ [shardctrler.NShards]ShardState
	data_         map[string]string
	client_seq_   map[int64]int
	client_id_    int64
	seq_          int
	handoffCh     chan Handoff
	done_         map[int]chan Reply
	doneMu        sync.Mutex
	lastApplied   int
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) copyClientSeq() map[int64]int {
	client_seq := make(map[int64]int)
	for cid, seq := range kv.client_seq_ {
		client_seq[cid] = seq
	}
	return client_seq
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	var client_seq map[int64]int
	var data map[string]string
	var shard_states [shardctrler.NShards]ShardState
	var last_conf Config
	var conf Config
	var groups map[int][]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&client_seq); e == nil {
		kv.client_seq_ = client_seq
	}
	if e := d.Decode(&data); e == nil {
		kv.data_ = data
	}
	if e := d.Decode(&shard_states); e == nil {
		kv.shard_states_ = shard_states
	}
	if e := d.Decode(&last_conf); e == nil {
		kv.last_config_ = last_conf
	}
	if e := d.Decode(&conf); e == nil {
		kv.config_ = conf
	}
	if e := d.Decode(&groups); e == nil {
		kv.groups_ = groups
	}
}

func (kv *ShardKV) DoApply() {
	for v := range kv.applyCh {
		if v.CommandValid {
			if latest, ok := v.Command.(Config); ok {
				kv.applyConfig(latest, v.CommandIndex)
			} else {
				val, err := kv.applyMsg(v)
				if kv.isLeader() {
					//DPrintf("%v", v)
					kv.doneMu.Lock()
					ch := kv.done_[v.CommandIndex]
					kv.doneMu.Unlock()
					if ch != nil {
						ch <- Reply{Value: val, Err: err}
					}
				}
			}

			if kv.maxraftstate_ != -1 && kv.rf.GetStateSize() >= kv.maxraftstate_ {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				kv.mu.Lock()
				if err := e.Encode(kv.client_seq_); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.data_); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.shard_states_); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.last_config_); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.config_); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.groups_); err != nil {
					panic(err)
				}
				kv.mu.Unlock()
				kv.rf.Snapshot(v.CommandIndex, w.Bytes())
			}
		} else if v.SnapshotValid {
			kv.lastApplied = v.SnapshotIndex
			kv.readSnapshot(v.Snapshot)
		}
	}
}

func (kv *ShardKV) applyConfig(latest Config, commandIndex int) {
	if commandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = commandIndex
	if latest.Conf.Num <= kv.config_.Conf.Num {
		return
	}
	kv.mu.Lock()
	kv.last_config_ = kv.config_
	kv.config_ = latest
	for gid, servers := range latest.Conf.Groups {
		kv.groups_[gid] = servers
	}

	handoff := make(map[int][]int) // gid -> shards
	for shard, gid := range kv.last_config_.Conf.Shards {
		target := kv.config_.Conf.Shards[shard]
		kv.config_.Conf.Shards[shard] = target
		if gid == kv.gid && target != kv.gid { // move from self to others
			handoff[target] = append(handoff[target], shard)
			kv.shard_states_[shard] = Pushing
		} else if gid != 0 && gid != kv.gid && target == kv.gid { // move from others to self
			kv.shard_states_[shard] = Pulling
		}
	}
	if kv.isLeader() {
		DPrintf("gid%d %v(%d)", kv.gid, kv.shard_states_, kv.config_.Conf.Num)
	}

	kv.mu.Unlock()
	if kv.isLeader() || kv.me == latest.Committer {
		kv.handoff(handoff, latest.Conf, kv.copyClientSeq())
	}
}

func (kv *ShardKV) handoff(handoff map[int][]int, latest shardctrler.Config, client_seq map[int64]int) {
	for gid, shards := range handoff {
		slice := make(map[string]string)
		for key := range kv.data_ {
			for _, shard := range shards {
				if key2shard(key) == shard {
					slice[key] = kv.data_[key]
				}
			}
		}
		kv.handoffCh <- Handoff{HandoffArgs{Num: latest.Num, Origin: kv.gid, Shards: shards, Data: slice, ClientSeq: client_seq}, gid, latest.Groups[gid]}
	}
}

func (kv *ShardKV) applyMsg(v raft.ApplyMsg) (string, ErrType) {
	if v.CommandIndex <= kv.lastApplied {
		return "", ErrTimeout
	}
	kv.lastApplied = v.CommandIndex
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var key string
	switch args := v.Command.(type) {
	case GetArgs:
		key = args.Key
		if err := kv.checkKeyL(key); err != OK {
			return "", err
		}
		return kv.data_[key], OK
	case PutAppendArgs:
		key = args.Key
		if err := kv.checkKeyL(key); err != OK {
			return "", err
		}
		if seq, ok := kv.client_seq_[args.ClientId]; ok && args.SeqNum <= seq {
			return "", OK
		}
		if args.Op == "Put" {
			kv.data_[key] = args.Value
		} else {
			kv.data_[key] += args.Value
		}
		kv.client_seq_[args.ClientId] = args.SeqNum
		return "", OK
	case HandoffArgs:
		if seq, ok := kv.client_seq_[args.ClientId]; ok && args.SeqNum <= seq {
			return "", OK
		}
		if args.Num > kv.config_.Conf.Num {
			return "", ErrTimeout
		} else if args.Num < kv.config_.Conf.Num {
			return "", OK
		}
		allServing := true
		for _, shard := range args.Shards {
			if kv.shard_states_[shard] != Serving {
				allServing = false
			}
		}
		if allServing {
			return "", OK
		}
		for k, v := range args.Data {
			kv.data_[k] = v
		}
		for _, shard := range args.Shards {
			kv.shard_states_[shard] = Serving
		}
		for cid, seq := range args.ClientSeq {
			if seq > kv.client_seq_[cid] {
				kv.client_seq_[cid] = seq
			}
		}
		if kv.isLeader() {
			DPrintf("handoff")
		}
		kv.client_seq_[args.ClientId] = args.SeqNum
		return "", OK
	case HandoffDoneArgs:
		if args.Num > kv.config_.Conf.Num {
			return "", ErrTimeout
		} else if args.Num < kv.config_.Conf.Num {
			return "", OK
		}
		for _, k := range args.Keys {
			delete(kv.data_, k)
		}
		for _, shard := range args.Shards {
			kv.shard_states_[shard] = Serving
		}
		if kv.isLeader() {
			DPrintf("handoff finish gid%d server%d", kv.gid, kv.me)
		}
		return "", OK
	default:
		panic("uncovered ApplyMsg")
	}
}

const (
	UpdateConfigInterval     = 100 * time.Millisecond
	UpdateConfigPollInterval = 200 * time.Millisecond
	TimeoutInterval          = 500 * time.Millisecond
)

func (kv *ShardKV) DoUpdateConfig() {
updateConfig:
	for {
		time.Sleep(UpdateConfigInterval)
		if !kv.isLeader() {
			continue
		}
		kv.mu.Lock()
		for _, state := range kv.shard_states_ {
			if state != Serving {
				kv.mu.Unlock()
				continue updateConfig
			}
		}
		num := kv.config_.Conf.Num + 1
		kv.mu.Unlock()
		kv.rf.Start(Config{kv.mck.Query(num), kv.me})
	}
}

func (kv *ShardKV) Handoff(args *HandoffArgs, reply *Reply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	_, reply.Err = kv.startAndWait("Handoff", *args)
	if reply.Err == OK {
		var doneArgs HandoffDoneArgs
		doneArgs.Num, doneArgs.Receiver, doneArgs.Shards = args.Num, kv.gid, args.Shards
		for s := range args.Data {
			doneArgs.Keys = append(doneArgs.Keys, s)
		}
		go kv.pollHandoffDone(doneArgs, args.Origin)
	}
}

func (kv *ShardKV) DoPollHandoff() {
	for handoff := range kv.handoffCh {
		handoff.args.ArgsMarker = ArgsMarker{ClientId: kv.client_id_, SeqNum: kv.seq_}
	nextHandoff:
		for {
			for _, si := range handoff.servers {
				var reply Reply
				ok := kv.sendHandoff(si, &handoff.args, &reply)
				if ok && reply.Err == OK {
					kv.seq_++
					break nextHandoff
				}
				if ok && reply.Err == ErrWrongGroup {
					panic("handoff reply.Err == ErrWrongGroup")
				}
			}
			time.Sleep(UpdateConfigPollInterval)
		}
	}
}

func (kv *ShardKV) HandoffDone(args *HandoffDoneArgs, reply *Reply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	_, reply.Err = kv.startAndWait("HandoffDone", *args)
}

func (kv *ShardKV) pollHandoffDone(args HandoffDoneArgs, origin int) {
	for {
		kv.mu.Lock()
		servers := kv.groups_[origin]
		kv.mu.Unlock()
		if len(servers) <= 0 {
			panic("no servers to HandoffDone")
		}
		for _, si := range servers {
			var reply Reply
			ok := kv.sendHandoffDone(si, &args, &reply)
			if ok && reply.Err == OK {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				panic("handoff reply.Err == ErrWrongGroup")
			}
		}
		time.Sleep(UpdateConfigPollInterval)
	}
}

func (kv *ShardKV) sendHandoff(si string, args *HandoffArgs, reply *Reply) bool {
	return kv.make_end(si).Call("ShardKV.Handoff", args, reply)
}

func (kv *ShardKV) sendHandoffDone(si string, args *HandoffDoneArgs, reply *Reply) bool {
	return kv.make_end(si).Call("ShardKV.HandoffDone", args, reply)
}

func (kv *ShardKV) Get(args *GetArgs, reply *Reply) {
	v, err := kv.Command("Get", args.Key, *args)
	reply.Err = err
	if err == OK {
		reply.Value = v
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *Reply) {
	_, reply.Err = kv.Command("PutAppend", args.Key, *args)
}

// Command args needs to be raw type (not pointer)
func (kv *ShardKV) Command(ty string, key string, args interface{}) (val string, err ErrType) {
	if !kv.isLeader() {
		return "", ErrWrongLeader
	}
	if err := kv.checkKey(key); err != OK {
		return "", err
	}
	return kv.startAndWait(ty, args)
}

// startAndWait args needs to be raw type (not pointer)
func (kv *ShardKV) startAndWait(ty string, cmd interface{}) (val string, err ErrType) {
	i, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return "", ErrWrongLeader
	}
	ch := make(chan Reply, 1)
	kv.doneMu.Lock()
	kv.done_[i] = ch
	kv.doneMu.Unlock()
	select {
	case reply := <-ch:
		return reply.Value, reply.Err
	case <-time.After(TimeoutInterval):
		return "", ErrTimeout
	}
}

func (kv *ShardKV) checkKey(key string) ErrType {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.checkKeyL(key)
}

func (kv *ShardKV) checkKeyL(key string) ErrType {
	shard := key2shard(key)
	if kv.config_.Conf.Shards[shard] == kv.gid {
		if kv.shard_states_[shard] == Serving {
			return OK
		}
		return ErrTimeout
	}
	return ErrWrongGroup
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) resumeHandoff() {
	handoff := make(map[int][]int) // gid -> shards
	for shard, gid := range kv.last_config_.Conf.Shards {
		if kv.shard_states_[shard] == Pushing {
			target := kv.config_.Conf.Shards[shard]
			if gid == kv.gid && target != kv.gid { // move from self to others
				handoff[target] = append(handoff[target], shard)
			}
		}
	}
	kv.handoff(handoff, kv.config_.Conf, kv.copyClientSeq())
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(Config{})
	labgob.Register(HandoffArgs{})
	labgob.Register(HandoffDoneArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate_ = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.serversLen = len(servers)
	rand.Seed(time.Now().UnixNano())
	kv.client_id_ = nrand()
	kv.seq_ = 100

	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.groups_ = make(map[int][]string)
	kv.data_ = make(map[string]string)
	kv.client_seq_ = make(map[int64]int)
	kv.done_ = make(map[int]chan Reply)
	kv.handoffCh = make(chan Handoff, shardctrler.NShards)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.resumeHandoff()
	go kv.DoApply()
	go kv.DoUpdateConfig()
	go kv.DoPollHandoff()

	return kv
}

const Padding = "    "
