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

func Min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) getLogEntry(index int) *LogEntry {
	return &rf.logs_[index-rf.last_include_index_]
}

func (rf *Raft) LastLogIndex() int {
	return rf.last_include_index_ + len(rf.logs_) - 1
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
	rf.logs_ = new_logs
}

func nextReelectTime() time.Time {
	return time.Now().Add(time.Duration(kMinElectionInterval+rand.Intn(kMaxElectionInterval-kMinElectionInterval)) * time.Millisecond)
}

func nextHeartBeatTime() time.Time {
	return time.Now().Add(100 * time.Millisecond)
}

func (rf *Raft) convert2Follower(term int) {
	rf.current_term_ = term
	rf.state_ = KFOLLOWER
	rf.vote_for_ = -1
	rf.reelect_time_ = nextReelectTime()
	rf.persist()
}

func (rf *Raft) convert2Leader() {
	rf.state_ = kLEADER
	rf.leaderID_ = rf.me
	rf.next_index_ = make([]int, rf.peernum_)
	rf.match_index_ = make([]int, rf.peernum_)
	rf.need_heart_beat_ = make([]bool, rf.peernum_)
	for i := 0; i < rf.peernum_; i++ {
		rf.next_index_[i] = rf.LastLogIndex() + 1
		rf.need_heart_beat_[i] = true
	}
	rf.heartbeat_time_ = nextHeartBeatTime()
	rf.replicate_conds_.Broadcast()
}

func (rf *Raft) convert2Candidate() {
	rf.state_ = KCANDIDATE
	rf.current_term_++
	rf.vote_for_ = rf.me
	rf.persist()
	rf.reelect_time_ = nextReelectTime()
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
		if rf.state_ != kLEADER && time.Now().After(rf.reelect_time_) {
			rf.convert2Candidate()
			last_log_index := rf.LastLogIndex()
			args := RequestVoteArgs{
				CandidateId: rf.me, Term: rf.current_term_, LastLogIndex: last_log_index, LastLogTerm: rf.getLogEntry(last_log_index).Term,
			}
			rf.mu.Unlock()

			cnt := 0
			for i := 0; i < rf.peernum_; i++ {
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
					if rf.state_ == KCANDIDATE && args.Term == rf.current_term_ {
						if rf.current_term_ < reply.Term {
							rf.convert2Follower(args.Term)
						} else if reply.VoteGrant {
							cnt++
							if cnt > rf.peernum_/2 {
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
		for rf.state_ != kLEADER || (rf.state_ == kLEADER && !rf.need_heart_beat_[server] && rf.next_index_[server] > rf.LastLogIndex()) {
			rf.replicate_conds_.Wait()
		}
		rf.need_heart_beat_[server] = false

		if rf.next_index_[server] <= rf.last_include_index_ {
			args := &InstallSnapshotArgs{
				Term: rf.current_term_, LeaderID: rf.me, LastLogTerm: rf.last_include_term_,
				LastLogIndex: rf.last_include_index_, Data: rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()
			reply := &InstallSnapshotReply{}

			ok := rf.sendInstallSnapshot(server, args, reply)
			if !ok {
				continue
			}

			rf.mu.Lock()
			if args.Term != rf.current_term_ {
			} else if rf.current_term_ < reply.Term {
				rf.convert2Follower(reply.Term)
			} else {
				rf.next_index_[server] = args.LastLogIndex + 1
			}
			rf.mu.Unlock()
			continue
		}

		prev_log_index := rf.next_index_[server] - 1
		var entries []LogEntry
		if prev_log_index >= rf.LastLogIndex() {
			entries = nil
		} else {
			entries = rf.logs_[rf.getLogPosition(prev_log_index)+1:]
		}
		args := &AppendEntriesArgs{
			Term: rf.current_term_, leaderID: rf.me, PrevLogIndex: prev_log_index,
			PrevLogTerm: rf.getLogEntry(prev_log_index).Term, Entries: entries, LeaderCommit: rf.commit_index_,
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			continue
		}

		rf.mu.Lock()
		if args.Term != rf.current_term_ {
		} else if rf.current_term_ < reply.Term {
			rf.convert2Follower(reply.Term)
		} else if !reply.Success {
			rf.next_index_[server] = reply.NextIndex
		} else {
			if entries != nil {
				rf.next_index_[server] += len(args.Entries)
				rf.match_index_[server] = rf.next_index_[server] - 1
				//DPrintf("leader%d send to server%d entries with lastindex%d", rf.me, server, rf.match_index_[server])

				var new_commit int = rf.commit_index_ + 1
				for ; new_commit <= rf.LastLogIndex(); new_commit++ {
					cnt := 0
					for j := 0; j < rf.peernum_; j++ {
						if j == rf.me || rf.match_index_[j] >= new_commit {
							cnt++
						}
					}
					if cnt <= rf.peernum_/2 {
						break
					}
				}
				new_commit -= 1
				if new_commit > rf.commit_index_ && rf.getLogEntry(new_commit).Term == rf.current_term_ {
					rf.commit_index_ = new_commit
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
		for rf.commit_index_ <= rf.apply_index_ {
			rf.apply_cond_.Wait()
		}
		for i := rf.apply_index_ + 1; i <= Min(rf.LastLogIndex(), rf.commit_index_); i++ {
			logentry := rf.getLogEntry(i)
			apply_msg := ApplyMsg{
				CommandValid: true, CommandIndex: i, CommandTerm: logentry.Term,
				Command: logentry.Content,
			}
			rf.mu.Unlock()
			rf.applyCh <- apply_msg
			rf.mu.Lock()
			rf.apply_index_++
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) activateHeartBeatThread() {
	for !rf.killed() {
		time.Sleep(kCheckInterval)
		rf.mu.Lock()
		if rf.state_ == kLEADER && time.Now().After(rf.heartbeat_time_) {
			for i := 0; i < rf.peernum_; i++ {
				rf.need_heart_beat_[i] = true
			}
			rf.heartbeat_time_ = nextHeartBeatTime()
			rf.replicate_conds_.Broadcast()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.current_term_)
	e.Encode(rf.vote_for_)
	e.Encode(rf.logs_)
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
		rf.current_term_ = currentTerm
		rf.vote_for_ = votedFor
		rf.logs_ = logs
		rf.last_include_index_ = last_include_index
		rf.last_include_term_ = last_include_term
		rf.apply_index_ = rf.last_include_index_
	}
}
