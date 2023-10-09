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
	CommandTerm  int

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

	current_term_ int
	vote_for_     int
	logs_         []LogEntry

	commit_index_ int
	apply_index_  int

	next_index_  []int
	match_index_ []int

	reelect_time_   time.Time
	heartbeat_time_ time.Time

	peernum_            int
	state_              int
	leaderID_           int
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
	if rf.state_ == kLEADER {
		isLeader = true
	}
	return rf.current_term_, isLeader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.state_ != kLEADER {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs_ = append(rf.logs_, LogEntry{command, rf.current_term_})
	rf.persist()
	rf.replicate_conds_.Broadcast()
	return rf.LastLogIndex(), rf.current_term_, true
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
	if rf.last_include_index_ >= index || rf.commit_index_ < index {
		return
	}

	rf.last_include_term_ = rf.getLogEntry(index).Term
	rf.discardLog(index, rf.last_include_term_)
	rf.last_include_index_ = index
	rf.commit_index_ = Max(index, rf.commit_index_)
	rf.apply_index_ = Max(index, rf.apply_index_)

	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
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
	leaderID     int
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
	reply.Term = rf.current_term_
	if rf.current_term_ > args.Term {
		reply.VoteGrant = false
		return
	}
	if rf.current_term_ < args.Term {
		rf.current_term_ = args.Term
		rf.state_ = KFOLLOWER
		rf.vote_for_ = -1
	}
	if (rf.vote_for_ == -1 || rf.vote_for_ == args.CandidateId) && !rf.newer(args) {
		rf.vote_for_ = args.CandidateId
		rf.persist()
		rf.reelect_time_ = nextReelectTime()
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
	reply.Term = rf.current_term_
	if args.Term < rf.current_term_ {
		reply.Success = false
		return
	}

	if rf.state_ != KFOLLOWER {
		rf.convert2Follower(args.Term)
	} else {
		rf.reelect_time_ = nextReelectTime()
		rf.current_term_ = args.Term
	}

	if !(args.PrevLogIndex <= rf.LastLogIndex() && args.PrevLogIndex >= rf.last_include_index_ && rf.getLogEntry(args.PrevLogIndex).Term == args.PrevLogTerm) {
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
	rf.leaderID_ = args.leaderID
	if args.Entries != nil {
		rf.logs_ = append(rf.logs_[:rf.getLogPosition(args.PrevLogIndex)+1], args.Entries...)
		//DPrintf("server%d recived %v\n%v", rf.me, args.Entries, rf.logs_)
		rf.persist()
	}
	if rf.commit_index_ < args.LeaderCommit {
		rf.commit_index_ = args.LeaderCommit
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
	reply.Term = rf.current_term_
	if args.Term < rf.current_term_ {
		return
	}

	if rf.current_term_ < args.Term {
		rf.current_term_ = args.Term
		rf.persist()
	}
	rf.reelect_time_ = nextReelectTime()

	if rf.last_include_index_ >= args.LastLogIndex {
		return
	}

	rf.discardLog(args.LastLogIndex, args.LastLogTerm)
	rf.last_include_index_ = args.LastLogIndex
	rf.last_include_term_ = args.LastLogTerm
	rf.commit_index_ = Max(rf.last_include_index_, rf.commit_index_)
	rf.apply_index_ = Max(rf.last_include_index_, rf.apply_index_)

	rf.persister.SaveStateAndSnapshot(rf.serializeState(), args.Data)
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

	rf.vote_for_ = -1
	rf.reelect_time_ = nextReelectTime()

	rf.peernum_ = len(rf.peers)
	rf.state_ = KFOLLOWER
	rf.apply_cond_ = sync.NewCond(&rf.mu)
	rf.replicate_conds_ = sync.NewCond(&rf.mu)

	rf.logs_ = append(rf.logs_, LogEntry{})
	rf.readPersist(persister.ReadRaftState())

	go rf.electionThread()
	for i := 0; i < rf.peernum_; i++ {
		if i == rf.me {
			continue
		}
		go rf.replicateThread(i)
	}
	go rf.applyThread()
	go rf.activateHeartBeatThread()

	return rf
}
