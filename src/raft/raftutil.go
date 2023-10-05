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

func min(x int, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func nextReelectTime() time.Time {
	return time.Now().Add(time.Duration(MINRELECTDCYCLE+rand.Intn(MINRELECTDCYCLE)) * time.Millisecond)
}

func nextHeartBeatTime() time.Time {
	return time.Now().Add(100 * time.Millisecond)
}

func (rf *Raft) convert2Follower(term int) {
	rf.CurrentTerm = term
	rf.state = FOLLOWER
	rf.VotedFor = -1
	rf.ReelectTime = nextReelectTime()
	rf.persist()
}

func (rf *Raft) convert2Leader() {
	DPrintf("leader%d (Term%d)\n", rf.me, rf.CurrentTerm)
	rf.state = LEADER
	rf.leaderID = rf.me
	rf.NextIndex = make([]int, rf.peernum)
	rf.MatchIndex = make([]int, rf.peernum)
	rf.need_heart_beat_ = make([]bool, rf.peernum)
	for i := 0; i < rf.peernum; i++ {
		rf.NextIndex[i] = len(rf.Logs)
		rf.need_heart_beat_[i] = true
	}
	rf.HeartBeatTime = nextHeartBeatTime()
	rf.replicate_conds_.Broadcast()
}

func (rf *Raft) convert2Candidate() {
	rf.state = CANDIDATE
	rf.CurrentTerm++
	DPrintf("candidate%d try to become leader in term:%d\n", rf.me, rf.CurrentTerm)
	rf.VotedFor = rf.me
	rf.persist()
	rf.ReelectTime = nextReelectTime()
}

func (rf *Raft) newer(args *RequestVoteArgs) bool {
	last_log_index_server := len(rf.Logs) - 1
	if rf.Logs[last_log_index_server].Term > args.LastLogTerm ||
		(rf.Logs[last_log_index_server].Term == args.LastLogTerm && last_log_index_server > args.LastLogIndex) {
		return true
	}
	return false
}

func (rf *Raft) electionThread() {
	for !rf.killed() {
		time.Sleep(CHECKINTERVAL)
		rf.mu.Lock()
		if rf.state != LEADER && time.Now().After(rf.ReelectTime) {
			rf.convert2Candidate()
			last_log_index := len(rf.Logs) - 1
			args := RequestVoteArgs{
				CandidateId: rf.me, Term: rf.CurrentTerm, LastLogIndex: last_log_index, LastLogTerm: rf.Logs[last_log_index].Term,
			}
			rf.mu.Unlock()

			cnt := 0
			for i := 0; i < rf.peernum; i++ {
				if i == rf.me {
					cnt++
					continue
				}
				go func(server int, args *RequestVoteArgs) {
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(server, args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state == CANDIDATE && args.Term == rf.CurrentTerm {
						if rf.CurrentTerm < reply.Term {
							rf.convert2Follower(args.Term)
						} else if reply.VoteGrant {
							cnt++
							if cnt > rf.peernum/2 {
								rf.convert2Leader()
							}
						}
					}
				}(i, &args)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicateThread(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.state != LEADER || (rf.state == LEADER && !rf.need_heart_beat_[server] && rf.NextIndex[server] > len(rf.Logs)-1) {
			rf.replicate_conds_.Wait()
		}
		rf.need_heart_beat_[server] = false

		for {
			prev_log_index := rf.NextIndex[server] - 1
			var entries []LogEntry
			if prev_log_index >= len(rf.Logs)-1 {
				entries = nil
			} else {
				entries = rf.Logs[prev_log_index+1:]
			}
			args := &AppendEntriesArgs{
				Term: rf.CurrentTerm, LeaderID: rf.me, PrevLogIndex: prev_log_index,
				PrevLogTerm: rf.Logs[prev_log_index].Term, Entries: entries, LeaderCommit: rf.CommitIndex,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}

			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				break
			}

			rf.mu.Lock()
			if args.Term != rf.CurrentTerm {
				rf.mu.Unlock()
				break
			}
			if rf.CurrentTerm < reply.Term {
				rf.convert2Follower(reply.Term)
				rf.mu.Unlock()
				break
			}
			if !reply.Success {
				rf.NextIndex[server] = reply.NextIndex
			} else {
				if entries != nil {
					rf.NextIndex[server] += len(args.Entries)
					rf.MatchIndex[server] = rf.NextIndex[server] - 1

					var new_commit int = rf.CommitIndex + 1
					for ; new_commit < len(rf.Logs); new_commit++ {
						cnt := 0
						for j := 0; j < rf.peernum; j++ {
							if j == rf.me || rf.MatchIndex[j] >= new_commit {
								cnt++
							}
						}
						if cnt <= rf.peernum/2 {
							break
						}
					}
					new_commit -= 1
					if new_commit > rf.CommitIndex && rf.Logs[new_commit].Term == rf.CurrentTerm {
						rf.CommitIndex = new_commit
						rf.apply_cond_.Broadcast()
					}
				}
				rf.mu.Unlock()
				break
			}
		}
	}
}

func (rf *Raft) applyThread() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.CommitIndex <= rf.ApplyIndex {
			rf.apply_cond_.Wait()
		}
		for i := rf.ApplyIndex + 1; i <= rf.CommitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true, CommandIndex: i, Command: rf.Logs[i].Content,
			}
		}
		rf.ApplyIndex = rf.CommitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) activateHeartBeatThread() {
	for !rf.killed() {
		time.Sleep(CHECKINTERVAL)
		rf.mu.Lock()
		if rf.state == LEADER && time.Now().After(rf.HeartBeatTime) {
			for i := 0; i < rf.peernum; i++ {
				rf.need_heart_beat_[i] = true
			}
			rf.HeartBeatTime = nextHeartBeatTime()
			rf.replicate_conds_.Broadcast()
		}
		rf.mu.Unlock()
	}
}
