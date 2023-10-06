package raft

import (
	//"fmt"

	"bytes"
	"math/rand"
	"time"

	"6.824/labgob"
)

type LogEntry struct {
	Content interface{}
	Term    int
}

func Max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) getLogEntry(index int) *LogEntry {
	return &rf.Logs[index-rf.last_include_index_]
}

func (rf *Raft) LastLogIndex() int {
	return rf.last_include_index_ + len(rf.Logs) - 1
}

func (rf *Raft) getLogPosition(index int) int {
	return index - rf.last_include_index_
}

func (rf *Raft) discardLog(last_include_index int, last_include_term int) {
	var new_logs []LogEntry
	if rf.LastLogIndex() > last_include_index {
		new_logs = make([]LogEntry, rf.LastLogIndex()-last_include_index+1)
		new_logs[0] = LogEntry{nil, last_include_term}
		for i := last_include_index + 1; i <= rf.LastLogIndex(); i++ {
			new_logs[i-last_include_index] = *rf.getLogEntry(i)
		}
	} else {
		new_logs = append(new_logs, LogEntry{nil, last_include_term})
	}
	rf.Logs = new_logs
}

func nextReelectTime() time.Time {
	return time.Now().Add(time.Duration(kMinElectionInterval+rand.Intn(kMaxElectionInterval-kMinElectionInterval)) * time.Millisecond)
}

func nextHeartBeatTime() time.Time {
	return time.Now().Add(100 * time.Millisecond)
}

func (rf *Raft) convert2Follower(term int) {
	rf.CurrentTerm = term
	rf.state = KFOLLOWER
	rf.VotedFor = -1
	rf.ReelectTime = nextReelectTime()
	rf.persist()
}

func (rf *Raft) convert2Leader() {
	rf.state = kLEADER
	rf.leaderID = rf.me
	rf.NextIndex = make([]int, rf.peernum)
	rf.MatchIndex = make([]int, rf.peernum)
	rf.need_heart_beat_ = make([]bool, rf.peernum)
	for i := 0; i < rf.peernum; i++ {
		rf.NextIndex[i] = rf.LastLogIndex() + 1
		rf.need_heart_beat_[i] = true
	}
	rf.HeartBeatTime = nextHeartBeatTime()
	rf.replicate_conds_.Broadcast()
}

func (rf *Raft) convert2Candidate() {
	rf.state = KCANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	rf.ReelectTime = nextReelectTime()
}

func (rf *Raft) newer(args *RequestVoteArgs) bool {
	last_log_index_server := rf.LastLogIndex()
	if rf.getLogEntry(last_log_index_server).Term > args.LastLogTerm ||
		(rf.getLogEntry(last_log_index_server).Term == args.LastLogTerm && last_log_index_server > args.LastLogIndex) {
		return true
	}
	return false
}

func (rf *Raft) electionThread() {
	for !rf.killed() {
		time.Sleep(kCheckInterval)
		rf.mu.Lock()
		if rf.state != kLEADER && time.Now().After(rf.ReelectTime) {
			rf.convert2Candidate()
			last_log_index := rf.LastLogIndex()
			args := RequestVoteArgs{
				CandidateId: rf.me, Term: rf.CurrentTerm, LastLogIndex: last_log_index, LastLogTerm: rf.getLogEntry(last_log_index).Term,
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
					if rf.state == KCANDIDATE && args.Term == rf.CurrentTerm {
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
		for rf.state != kLEADER || (rf.state == kLEADER && !rf.need_heart_beat_[server] && rf.NextIndex[server] > rf.LastLogIndex()) {
			rf.replicate_conds_.Wait()
		}
		rf.need_heart_beat_[server] = false

		if rf.NextIndex[server] <= rf.last_include_index_ {
			args := &InstallSnapshotArgs{
				Term: rf.CurrentTerm, LeaderID: rf.me, LastLogTerm: rf.last_include_term_,
				LastLogIndex: rf.last_include_index_, Data: rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()
			reply := &InstallSnapshotReply{}

			ok := rf.sendInstallSnapshot(server, args, reply)
			if !ok {
				continue
			}

			rf.mu.Lock()
			if args.Term != rf.CurrentTerm {
			} else if rf.CurrentTerm < reply.Term {
				rf.convert2Follower(reply.Term)
			} else {
				rf.NextIndex[server] = args.LastLogIndex + 1
			}
			rf.mu.Unlock()
			continue
		}

		prev_log_index := rf.NextIndex[server] - 1
		var entries []LogEntry
		if prev_log_index >= rf.LastLogIndex() {
			entries = nil
		} else {
			entries = rf.Logs[rf.getLogPosition(prev_log_index)+1:]
		}
		args := &AppendEntriesArgs{
			Term: rf.CurrentTerm, LeaderID: rf.me, PrevLogIndex: prev_log_index,
			PrevLogTerm: rf.getLogEntry(prev_log_index).Term, Entries: entries, LeaderCommit: rf.CommitIndex,
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			continue
		}

		rf.mu.Lock()
		if args.Term != rf.CurrentTerm {
		} else if rf.CurrentTerm < reply.Term {
			rf.convert2Follower(reply.Term)
		} else if !reply.Success {
			rf.NextIndex[server] = reply.NextIndex
		} else {
			if entries != nil {
				rf.NextIndex[server] += len(args.Entries)
				rf.MatchIndex[server] = rf.NextIndex[server] - 1
				//DPrintf("leader%d send to server%d entries with lastindex%d", rf.me, server, rf.MatchIndex[server])

				var new_commit int = rf.CommitIndex + 1
				for ; new_commit <= rf.LastLogIndex(); new_commit++ {
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
				if new_commit > rf.CommitIndex && rf.getLogEntry(new_commit).Term == rf.CurrentTerm {
					rf.CommitIndex = new_commit
					rf.apply_cond_.Broadcast()
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyThread() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.CommitIndex <= rf.ApplyIndex {
			rf.apply_cond_.Wait()
		}
		for i := rf.ApplyIndex + 1; i <= rf.CommitIndex; i++ {
			apply_msg := ApplyMsg{
				CommandValid: true, CommandIndex: i, Command: rf.getLogEntry(i).Content,
			}
			rf.mu.Unlock()
			rf.applyCh <- apply_msg
			rf.mu.Lock()
		}
		rf.ApplyIndex = rf.CommitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) activateHeartBeatThread() {
	for !rf.killed() {
		time.Sleep(kCheckInterval)
		rf.mu.Lock()
		if rf.state == kLEADER && time.Now().After(rf.HeartBeatTime) {
			for i := 0; i < rf.peernum; i++ {
				rf.need_heart_beat_[i] = true
			}
			rf.HeartBeatTime = nextHeartBeatTime()
			rf.replicate_conds_.Broadcast()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.last_include_index_)
	e.Encode(rf.last_include_term_)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.serializeState())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, last_include_index, last_include_term int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil || d.Decode(&last_include_index) != nil || d.Decode(&last_include_term) != nil {
		DPrintf("decoding wrong")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
		rf.last_include_index_ = last_include_index
		rf.last_include_term_ = last_include_term
		rf.ApplyIndex = rf.last_include_index_
	}
}
