package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func makeConfig(num int) *Config {
	config := new(Config)
	config.Num = num
	config.Groups = map[int][]string{}
	for i := 1; i < len(config.Shards); i++ {
		config.Shards[i] = i
	}
	return config
}

const (
	OK                = "OK"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeout        = "ErrTimeout"
	ErrUnexpectedType = "ErrUnexpectedType"
	ErrNoGroup        = "ErrNoGroup"
	ErrGroupExisted   = "ErrGroupExisted"
)

type Err string

type JoinArgs struct {
	ClerkId int64
	CmdId   int64
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	ClerkId int64
	CmdId   int64
	GIDs    []int
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	ClerkId int64
	CmdId   int64
	Shard   int
	GID     int
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	ClerkId int64
	CmdId   int64
	Num     int // desired config number
}

type QueryReply struct {
	Err    Err
	Config Config
}

type CmdArgs struct {
	Type    CmdType
	ClerkId int64
	CmdId   int64
	Servers map[int][]string // new GID -> servers mappings
	GIDs    []int
	Shard   int
	Num     int // desired config number
}

type CmdReply struct {
	Err    Err
	Config Config
}

type Cmd struct {
	*CmdArgs
}

type CmdType uint8

const (
	CmdQuery CmdType = iota
	CmdJoin
	CmdLeave
	CmdMove
)

func (t CmdType) toString() string {
	switch t {
	case CmdQuery:
		return "Query"
	case CmdJoin:
		return "Join"
	case CmdLeave:
		return "Leave"
	case CmdMove:
		return "Move"
	}
	return ""
}
