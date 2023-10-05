package raft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const MINRELECTDCYCLE int = 500
const CHECKINTERVAL time.Duration = 10 * time.Millisecond
const HEARTBEATINTERVAL time.Duration = 100 * time.Millisecond
const LEADER int = 0
const CANDIDATE int = 1
const FOLLOWER int = 2

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32
	applyCh   chan ApplyMsg

	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	CommitIndex int
	ApplyIndex  int

	NextIndex  []int
	MatchIndex []int

	ReelectTime   time.Time
	HeartBeatTime time.Time

	peernum          int
	state            int
	leaderID         int
	apply_cond_      *sync.Cond
	replicate_conds_ *sync.Cond
	need_heart_beat_ []bool
}

func (rf *Raft) GetState() (int, bool) {
	var isLeader bool = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isLeader = true
	}
	return rf.CurrentTerm, isLeader
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("decoding wrong")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term      int
	VoteGrant bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		reply.VoteGrant = false
		return
	}
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.state = FOLLOWER
		rf.VotedFor = -1
	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && !rf.newer(args) {
		rf.VotedFor = args.CandidateId
		rf.persist()
		rf.ReelectTime = nextReelectTime()
		reply.VoteGrant = true
	} else {
		reply.VoteGrant = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		DPrintf("server%d(Term%d) reject outdate leader %d(Term%d)\n", rf.me, rf.CurrentTerm, args.LeaderID, args.Term)
		return
	}

	if rf.state != FOLLOWER {
		rf.convert2Follower(args.Term)
	} else {
		rf.ReelectTime = nextReelectTime()
		rf.CurrentTerm = args.Term
	}

	if !(args.PrevLogIndex <= len(rf.Logs)-1 && rf.Logs[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Success = false
		var next_index int
		if args.PrevLogIndex > len(rf.Logs)-1 {
			next_index = len(rf.Logs)
		} else {
			for next_index = args.PrevLogIndex; next_index > 0 && rf.Logs[next_index].Term == rf.Logs[next_index-1].Term; next_index-- {
			}
		}
		reply.NextIndex = next_index
		return
	}

	reply.Success = true
	rf.leaderID = args.LeaderID
	if args.Entries != nil {
		rf.Logs = append(rf.Logs[:args.PrevLogIndex+1], args.Entries...)
		rf.persist()
	}
	if rf.CommitIndex < args.LeaderCommit {
		rf.CommitIndex = args.LeaderCommit
		rf.apply_cond_.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != LEADER {
		return -1, -1, false
	}
	rf.mu.Lock()
	rf.Logs = append(rf.Logs, LogEntry{command, rf.CurrentTerm})
	rf.persist()
	rf.replicate_conds_.Broadcast()
	rf.mu.Unlock()
	return len(rf.Logs) - 1, rf.CurrentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.Logs = []LogEntry{}
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.NextIndex = nil
	rf.MatchIndex = nil
	rf.CommitIndex = 0
	rf.ApplyIndex = 0
	rf.ReelectTime = nextReelectTime()

	rf.peernum = len(rf.peers)
	rf.state = FOLLOWER
	rf.apply_cond_ = sync.NewCond(&rf.mu)
	rf.replicate_conds_ = sync.NewCond(&rf.mu)

	rf.Logs = append(rf.Logs, LogEntry{nil, 0})
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	go rf.electionThread()
	for i := 0; i < rf.peernum; i++ {
		if i == rf.me {
			continue
		}
		go rf.replicateThread(i)
	}
	go rf.applyThread()
	go rf.activateHeartBeatThread()

	return rf
}
