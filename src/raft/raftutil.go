package raft

import (
	//"fmt"

	"math/rand"
	"time"
)

type LogEntry struct {
	Content interface{}
	Term    int
}

func nextReelectTime() time.Time {
	return time.Now().Add(time.Duration((MINRELECTDCYCLE + 5*rand.Intn(MINRELECTDCYCLE/5))) * time.Millisecond)
}

func nextHeartBeatTime() time.Time {
	return time.Now().Add(100 * time.Millisecond)
}

func (rf *Raft) convert2Follower(term int) {
	rf.CurrentTerm = term
	rf.state = FOLLOWER
	rf.VotedFor = -1
	rf.ReelectTime = nextReelectTime()
}

func (rf *Raft) convert2Leader() {
	//fmt.Printf("leader%d (Term%d)\n", rf.me, rf.CurrentTerm)
	rf.state = LEADER
	rf.leaderID = rf.me
	rf.NextIndex = make([]int, rf.peernum)
	for i := 0; i < rf.peernum; i++ {
		rf.NextIndex[i] = len(rf.Logs)
	}
	rf.MatchIndex = make([]int, rf.peernum)
	rf.HeartBeatTime = time.Now()
}

func (rf *Raft) convert2Candidate() {
	rf.state = CANDIDATE
	rf.CurrentTerm++
	//fmt.Printf("candidate%d try to become leader in term:%d\n", rf.me, rf.CurrentTerm)
	rf.VotedFor = rf.me
	rf.ReelectTime = nextReelectTime()
}

func (rf *Raft) newer(args *RequestVoteArgs) bool {
	lastLogIndex := len(rf.Logs) - 1
	if rf.Logs[lastLogIndex].Term > args.LastLogTerm ||
		(rf.Logs[lastLogIndex].Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return true
	}
	return false
}

func (rf *Raft) requestVoteMonitor() bool {
	voteReplies := make([]RequestVoteReply, rf.peernum)

	for i := 0; i < rf.peernum; i++ {
		if i == rf.me {
			voteReplies[i].Term = rf.CurrentTerm
			voteReplies[i].VoteGrant = true
		} else {
			lastLogIndex := len(rf.Logs) - 1
			args := &RequestVoteArgs{rf.me, rf.CurrentTerm, lastLogIndex, rf.Logs[lastLogIndex].Term}
			reply := &(voteReplies[i])
			go rf.sendRequestVote(i, args, reply)
		}
	}

	for !rf.killed() && rf.state == CANDIDATE {
		cnt := 0
		time.Sleep(CHECKINTERVAL)
		if time.Now().After(rf.ReelectTime) {
			return false
		}
		for _, reply := range voteReplies {
			if reply.Term > rf.CurrentTerm {
				rf.convert2Follower(reply.Term)
				return false
			}
			if reply.VoteGrant {
				cnt++
			}
			if cnt > rf.peernum/2 {
				return true
			}
		}
	}
	return false
}

func (rf *Raft) increCommit() int {
	var increment int = 1
	for {
		target := rf.CommitIndex + increment
		var cnt int = 0
		for index, value := range rf.MatchIndex {
			if value >= target || index == rf.me {
				cnt++
			}
		}
		if cnt <= rf.peernum/2 {
			return increment - 1
		}
		increment++
	}
}

func (rf *Raft) sendApply(newCommit int) {
	for i := newCommit - 1; i >= 0; i-- {
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.CommandIndex = rf.CommitIndex - i
		applyMsg.Command = rf.Logs[applyMsg.CommandIndex].Content
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) tryAppend(server int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	prevLogIndex := rf.NextIndex[server] - 1
	var entries []LogEntry
	if prevLogIndex == len(rf.Logs)-1 {
		entries = nil
	} else {
		entries = append(entries, rf.Logs[prevLogIndex+1:]...)
	}
	args := &AppendEntriesArgs{rf.CurrentTerm, rf.me, prevLogIndex,
		rf.Logs[prevLogIndex].Term, entries, rf.CommitIndex}
	rf.mu.Unlock()
	//fmt.Printf("leader%d send to server%d\n", rf.me, server)
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	} else {
		rf.mu.Unlock()
	}
	ok := rf.sendAppendEntries(server, args, reply)
	rf.mu.Lock()
	if rf.state == LEADER {
		if ok {
			if reply.Term > rf.CurrentTerm {
				//fmt.Printf("leader%d become to follower\n", rf.me)
				rf.convert2Follower(reply.Term)
				rf.mu.Unlock()
			} else if !reply.Success {
				rf.NextIndex[server]--
				rf.mu.Unlock()
				rf.tryAppend(server, reply)
			} else {
				if entries != nil && (rf.MatchIndex[server] < prevLogIndex+len(entries)) {
					rf.MatchIndex[server] = prevLogIndex + len(entries)
					rf.NextIndex[server] = rf.MatchIndex[server] + 1
					if rf.CurrentTerm == entries[len(entries)-1].Term {
						increment := rf.increCommit()
						if increment > 0 {
							rf.CommitIndex += increment
							rf.sendApply(increment)
						}
					}
				}
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(HEARTBEATINTERVAL)
			rf.tryAppend(server, reply)
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendEntriesHepler() {
	appendReplies := make([]AppendEntriesReply, rf.peernum)
	for i := 0; i < rf.peernum; i++ {
		if i != rf.me {
			go rf.tryAppend(i, &(appendReplies[i]))
		}
	}
}
