package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrDoneCommandId = "ErrDoneCommandId"
	ErrTimeout       = "ErrTimeout"
)

type Err string

type Reply struct {
	Err Err
}

// Put or Append
type PutAppendArgs struct {
	Op        string // "Put" or "Append"
	Key       string
	Value     string
	ClerkId   int64
	CommandId uint64
}

type PutAppendReply struct {
	Reply
}

type GetArgs struct {
	Key       string
	ClerkId   int64
	CommandId uint64
}

type GetReply struct {
	Reply
	Value string
}
