package kvraft

type CmdType uint8

const (
	CmdGet CmdType = iota
	CmdPut
	CmdAppend
)

func (t CmdType) toString() string {
	switch t {
	case CmdGet:
		return "Get"
	case CmdPut:
		return "Put"
	case CmdAppend:
		return "Append"
	}
	return ""
}

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

// CmdPut or CmdAppend
type PutAppendArgs struct {
	Op        string // "CmdPut" or "CmdAppend"
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

type CmdArgs struct {
	Type    CmdType
	Key     string
	Value   string
	ClerkId int64
	CmdId   uint64
}

type CmdReply struct {
	Err   Err
	Value string
}

type Cmd struct {
	*CmdArgs
}
