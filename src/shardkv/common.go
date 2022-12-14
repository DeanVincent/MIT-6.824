package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongCmdId      = "ErrWrongCmdId"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrTimeOut         = "ErrTimeOut"
	ErrWrongConfigNum  = "ErrWrongConfigNum"
	ErrWrongShardState = "ErrWrongShardState"
)

type Err string

type OpRequest struct {
	Type    OpType
	Key     string
	Value   string
	ClerkId int64
	CmdId   int64
}

type OpResponse struct {
	Err   Err
	Value string
}

type PullShardRequest struct {
	ConfigNum int
	ShardId   int
}

type PullShardResponse struct {
	Err         Err
	KVs         map[string]string
	LastApplied map[int64]*OpReqWithResp
}

type InsertShardRequest struct {
	ConfigNum   int
	ShardId     int
	KVs         map[string]string
	LastApplied map[int64]*OpReqWithResp
}

type InsertShardResponse struct {
	Err Err
}

type DeleteShardRequest struct {
	ConfigNum int
	ShardId   int
}

type DeleteShardResponse struct {
	Err Err
}

type UpdateConfigRequest struct {
	Config *shardctrler.Config
}

type UpdateConfigResponse struct {
	Err Err
}
