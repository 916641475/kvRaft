package kvraft

import "time"

const (
	OK             = "OK"
	Fail           = "Fail"
	ErrWrongLeader = "ErrWrongLeader"
	kCheckInterval = time.Duration(10 * time.Millisecond)
	kTimeoutThsh   = time.Duration(300 * time.Millisecond)
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqNum   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
