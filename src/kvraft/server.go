package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType   string
	Key      string
	Value    string
	ClientId int64
	SeqNum   int
}

type KVServer struct {
	mu_            sync.Mutex
	cond_          sync.Cond
	me_            int
	rf_            *raft.Raft
	applyCh_       chan raft.ApplyMsg
	dead           int32
	data_          map[string]string
	client_seq_    map[int64]int
	index_toapply_ int
	term_toapply_  int

	maxraftstate int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{OpType: "Get", Key: args.Key}
	index, term, is_leader := kv.rf_.Start(op)
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu_.Lock()
	for index != kv.index_toapply_ {
		kv.cond_.Wait()
	}
	if term != kv.term_toapply_ {
		reply.Err = Fail
	} else {
		reply.Err = OK
		reply.Value = kv.data_[args.Key]
	}
	kv.mu_.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		OpType: args.Op, Key: args.Key, Value: args.Value,
		ClientId: args.ClientId, SeqNum: args.SeqNum}
	index, term, is_leader := kv.rf_.Start(op)
	if !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu_.Lock()
	for index != kv.index_toapply_ {
		kv.cond_.Wait()
	}
	if term != kv.term_toapply_ {
		reply.Err = Fail
	} else {
		reply.Err = OK
	}
	kv.mu_.Unlock()
}

func (kv *KVServer) ListenThread() {
	for !kv.killed() {
		apply_msg := <-kv.applyCh_
		if apply_msg.CommandValid {
			kv.mu_.Lock()
			op_toapply_ := apply_msg.Command.(Op)
			if op_toapply_.OpType != "Get" && kv.client_seq_[op_toapply_.ClientId] < op_toapply_.SeqNum {
				kv.client_seq_[op_toapply_.ClientId] = op_toapply_.SeqNum
				if op_toapply_.OpType == "Put" {
					kv.data_[op_toapply_.Key] = op_toapply_.Value
				} else {
					kv.data_[op_toapply_.Key] += op_toapply_.Value
				}
			}
			if _, is_leader := kv.rf_.GetState(); is_leader {
				kv.index_toapply_ = apply_msg.CommandIndex
				kv.term_toapply_ = apply_msg.CommandTerm
				kv.mu_.Unlock()
				kv.cond_.Broadcast()
			} else {
				kv.mu_.Unlock()
			}
			if kv.rf_.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
				kv.mu_.Lock()
				kv.MakeSnapshot(apply_msg.CommandIndex)
				kv.mu_.Unlock()
			}
		} else {
			kv.mu_.Lock()
			kv.ReadSnapshot(apply_msg.Snapshot)
			kv.mu_.Unlock()
		}
	}
}

func (kv *KVServer) MakeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data_)
	e.Encode(kv.client_seq_)
	kv.rf_.Snapshot(index, w.Bytes())
}

func (kv *KVServer) ReadSnapshot(snapshot []byte) {
	var data map[string]string
	var client_seq map[int64]int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&data); e != nil {
		data = make(map[string]string)
	}
	if e := d.Decode(&client_seq); e != nil {
		client_seq = make(map[int64]int)
	}
	kv.data_ = data
	kv.client_seq_ = client_seq
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf_.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me_ int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me_ = me_
	kv.maxraftstate = maxraftstate
	kv.cond_ = *sync.NewCond(&kv.mu_)
	kv.applyCh_ = make(chan raft.ApplyMsg)
	kv.rf_ = raft.Make(servers, me_, persister, kv.applyCh_)
	kv.ReadSnapshot(persister.ReadSnapshot())
	go kv.ListenThread()
	return kv
}
