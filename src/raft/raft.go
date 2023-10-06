package raft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const kMinElectionInterval int = 600
const kMaxElectionInterval int = 1000
const kCheckInterval time.Duration = 10 * time.Millisecond
const kHeartBeatInterval time.Duration = 100 * time.Millisecond
const kLEADER int = 0
const KCANDIDATE int = 1
const KFOLLOWER int = 2

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

	peernum             int
	state               int
	leaderID            int
	apply_cond_         *sync.Cond
	replicate_conds_    *sync.Cond
	need_heart_beat_    []bool
	last_include_term_  int
	last_include_index_ int
}

func (rf *Raft) GetState() (int, bool) {
	var isLeader bool = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == kLEADER {
		isLeader = true
	}
	return rf.CurrentTerm, isLeader
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.last_include_index_ >= index || rf.CommitIndex < index {
		return
	}

	rf.last_include_term_ = rf.getLogEntry(index).Term
	rf.discardLog(index, rf.last_include_term_)
	rf.last_include_index_ = index
	rf.CommitIndex = Max(index, rf.CommitIndex)
	rf.ApplyIndex = Max(index, rf.ApplyIndex)

	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	DPrintf("server%d snapshot with lastindex%d", rf.me, rf.last_include_index_)
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

type InstallSnapshotArgs struct {
	Term         int
	LeaderID     int
	LastLogIndex int
	LastLogTerm  int
	Data         []byte
}

type InstallSnapshotReply struct {
	Term int
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
		rf.state = KFOLLOWER
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
		return
	}

	if rf.state != KFOLLOWER {
		rf.convert2Follower(args.Term)
	} else {
		rf.ReelectTime = nextReelectTime()
		rf.CurrentTerm = args.Term
	}

	if !(args.PrevLogIndex <= rf.LastLogIndex() && rf.getLogEntry(args.PrevLogIndex).Term == args.PrevLogTerm) {
		reply.Success = false
		var next_index int
		if args.PrevLogIndex > rf.LastLogIndex() {
			next_index = rf.LastLogIndex() + 1
		} else {
			next_index = args.PrevLogIndex
			for {
				if !(next_index > rf.last_include_index_ && rf.getLogEntry(next_index).Term == rf.getLogEntry(next_index-1).Term) {
					break
				}
				next_index--
			}
		}
		reply.NextIndex = next_index
		return
	}

	reply.Success = true
	rf.leaderID = args.LeaderID
	//DPrintf("server%d recived from leader%d", rf.me, args.LeaderID)
	if args.Entries != nil {
		rf.Logs = append(rf.Logs[:rf.getLogPosition(args.PrevLogIndex)+1], args.Entries...)
		//DPrintf("server%d recived %v\n%v", rf.me, args.Entries, rf.Logs)
		rf.persist()
	}
	if rf.CommitIndex < args.LeaderCommit {
		rf.CommitIndex = args.LeaderCommit
		rf.apply_cond_.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//return ok
	ok := false
	begin_time := time.Now()
	go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}(server, args, reply)
	for {
		time.Sleep(kCheckInterval)
		if time.Now().After(begin_time.Add(kHeartBeatInterval)) || ok {
			return ok
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	rf.ReelectTime = nextReelectTime()

	if rf.last_include_index_ >= args.LastLogIndex {
		return
	}

	rf.discardLog(args.LastLogIndex, args.LastLogTerm)
	rf.last_include_index_ = args.LastLogIndex
	rf.last_include_term_ = args.LastLogTerm
	rf.CommitIndex = Max(rf.last_include_index_, rf.CommitIndex)
	rf.ApplyIndex = Max(rf.last_include_index_, rf.ApplyIndex)

	rf.persister.SaveStateAndSnapshot(rf.serializeState(), args.Data)
	//DPrintf("server%d install snapshot with lastindex%d", rf.me, rf.last_include_index_)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.last_include_term_,
		SnapshotIndex: rf.last_include_index_,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	//return ok
	ok := false
	begin_time := time.Now()
	go func(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}(server, args, reply)
	for {
		time.Sleep(kCheckInterval)
		if time.Now().After(begin_time.Add(kHeartBeatInterval)) || ok {
			return ok
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state != kLEADER {
		return -1, -1, false
	}
	rf.mu.Lock()
	rf.Logs = append(rf.Logs, LogEntry{command, rf.CurrentTerm})
	rf.persist()
	rf.replicate_conds_.Broadcast()
	rf.mu.Unlock()
	return rf.LastLogIndex(), rf.CurrentTerm, true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.VotedFor = -1
	rf.ReelectTime = nextReelectTime()

	rf.peernum = len(rf.peers)
	rf.state = KFOLLOWER
	rf.apply_cond_ = sync.NewCond(&rf.mu)
	rf.replicate_conds_ = sync.NewCond(&rf.mu)

	rf.Logs = append(rf.Logs, LogEntry{})
	rf.readPersist(persister.ReadRaftState())
	DPrintf("lastindex%d", rf.last_include_index_)

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
