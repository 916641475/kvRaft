package raft

import (
	//	"bytes"

	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"

	"time"
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
	LastApply   int

	NextIndex  []int
	MatchIndex []int

	ReelectTime   time.Time
	HeartBeatTime time.Time

	peernum  int
	state    int
	leaderID int
}

func (rf *Raft) GetState() (int, bool) {
	var isLeader bool = false
	if rf.state == LEADER {
		isLeader = true
	}
	return rf.CurrentTerm, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
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
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term || rf.newer(args) || (rf.CurrentTerm == args.Term && rf.VotedFor != -1) {
		reply.VoteGrant = false
		if rf.CurrentTerm < args.Term {
			rf.CurrentTerm = args.Term
		}
	} else {
		rf.convert2Follower(args.Term)
		rf.VotedFor = args.CandidateId
		//fmt.Printf("server%d vote for candidate%d in term:%d\n", rf.me, args.CandidateId, rf.CurrentTerm)
		reply.VoteGrant = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		//fmt.Printf("server%d reject outdate leader %d\n", rf.me, args.LeaderID)
		rf.mu.Unlock()
		return
	}

	rf.ReelectTime = nextReelectTime()
	if rf.state != FOLLOWER {
		rf.convert2Follower(args.Term)
	}

	if !(args.PrevLogIndex <= len(rf.Logs)-1 && rf.Logs[args.PrevLogIndex].Term == args.PrevLogTerm) {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	reply.Success = true
	if args.Entries != nil &&
		!(rf.leaderID == args.LeaderID && len(rf.Logs) >= args.PrevLogIndex+len(args.Entries)+1) {
		rf.Logs = append(rf.Logs[:args.PrevLogIndex+1], args.Entries...)
		//fmt.Printf("(Term%d) server%d : %v\n", rf.CurrentTerm, rf.me, rf.Logs)
	}
	if rf.CommitIndex < args.LeaderCommit {
		increment := args.LeaderCommit - rf.CommitIndex
		rf.CommitIndex = args.LeaderCommit
		rf.sendApply(increment)
	}
	rf.leaderID = args.LeaderID
	rf.mu.Unlock()
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
	rf.mu.Unlock()
	go rf.appendEntriesHepler()
	return len(rf.Logs) - 1, rf.CurrentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(CHECKINTERVAL)
		rf.mu.Lock()
		if rf.state != LEADER && time.Now().After(rf.ReelectTime) {
			rf.convert2Candidate()
			rf.mu.Unlock()
			if rf.requestVoteMonitor() {
				rf.mu.Lock()
				rf.convert2Leader()
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		if rf.state == LEADER && time.Now().After(rf.HeartBeatTime) {
			go rf.appendEntriesHepler()
			rf.HeartBeatTime = nextHeartBeatTime()
		}
		rf.mu.Unlock()
	}
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
	rf.CommitIndex = -1
	rf.LastApply = -1
	rf.ReelectTime = nextReelectTime()

	rf.peernum = len(rf.peers)
	rf.state = FOLLOWER

	rf.Logs = append(rf.Logs, LogEntry{-1, 0})
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
