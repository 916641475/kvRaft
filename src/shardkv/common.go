package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type ErrType int

const (
	OK ErrType = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrTimeout
)

type ShardState int

const (
	Serving ShardState = iota
	Pulling
	Pushing
)

type ArgsMarker struct {
	ClientId int64
	SeqNum   int
}

type PutAppendArgs struct {
	ArgsMarker
	Key   string
	Value string
	Op    string
}

type GetArgs struct {
	ArgsMarker
	Key string
}

type Handoff struct {
	args    HandoffArgs
	target  int
	servers []string
}

type HandoffArgs struct {
	ArgsMarker
	Num       int
	Origin    int
	Shards    []int
	Data      map[string]string
	ClientSeq map[int64]int
}

type HandoffDoneArgs struct {
	Num      int
	Receiver int
	Keys     []string
	Shards   []int
}

type Reply struct {
	Value string
	Err   ErrType
}
