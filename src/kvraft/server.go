// Q: Clerk 有时不知道哪个是 leader。如果 Clerk 向错误的 kvserver 发送RPC，或者无法到达kvserver，书记员应该通过向不同的kvserver发送来重新尝试。
// 或者 kvserver 可以将自己认为的 leader 返回给 Clerk

// 当 Raft 日志条目数量超过 maxRaftState 时, KVServer 命令 Raft 进行快照.
//
// 快照包含的数据有: Data, CommandIds
//
// Raft 要求 KVServer 安装快照
//
// 场景一: Raft apply Snapshot(index: y) -> Raft apply Command c1(index: x)
// 		  其中, x < y, 状态机应该识别 c1 是重复的, 通过 c1.ClerkId, c1.CmdId 与 kv.CommandIds相比较得出

package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// @deprecated see Cmd in common.go
type Op struct {
	Type      CmdType
	Key       string
	Value     string
	ClerkId   int64
	CommandId uint64
}

type ClerkCommandPair struct {
	ClerkId   int64
	CommandId uint64
}

type ApplyResult struct {
	Err   Err
	Value string
}

type CommandNotify struct {
	clerkId   int64
	commandId uint64
	notifyCh  chan *ApplyResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	// Your definitions here.

	Data map[string]string // state, kv pairs
	// 不仅仅是保存最新的commandId, 还要保存响应的执行结果, 不能默认是成功的, 因为有可能执行失败了, 再次查询时需要告诉客户端这个结果
	CommandIds map[int64]CommandAppliedRecord // state, last cmdId and result for each Clerk
	notifies   map[int]CommandNotify          // op and result for each log index

	persister *raft.Persister
}

type CommandAppliedRecord struct {
	MaxAppliedCommandId uint64       // cmdId
	Result              *ApplyResult // command applied result
}

const ExecuteTimeout = 1000 * time.Millisecond

func (kv *KVServer) Cmd(args *CmdArgs, reply *CmdReply) {
	DPrintf("{Node %v} receives request %v", kv.me, *args)
	defer func() {
		DPrintf("{Node %v} complete executing request %v and reply %v", kv.me, *args, *reply)
	}()

	kv.mu.Lock()
	if kv.isDuplicatedCommand(args.ClerkId, args.CmdId) {
		switch args.Type {
		case CmdGet:
			reply.Err = ErrDoneCommandId
		case CmdPut, CmdAppend:
			lastAppliedResult := kv.CommandIds[args.ClerkId]
			if args.CmdId == lastAppliedResult.MaxAppliedCommandId {
				reply.Err = lastAppliedResult.Result.Err
			} else {
				reply.Err = ErrDoneCommandId
			}
		default:
			DPrintf("{Node %v} receives unexpected request %v", kv.me, *args)
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Cmd{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notify := CommandNotify{
		clerkId:   args.ClerkId,
		commandId: args.CmdId,
		notifyCh:  make(chan *ApplyResult, 1),
	}
	kv.mu.Lock()
	kv.notifies[index] = notify
	kv.mu.Unlock()

	select {
	case result := <-notify.notifyCh:
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifies, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("{Node %v} receives request %v", kv.me, args)
	defer func() {
		DPrintf("{Node %v} receives request %v and reply %v", kv.me, args, reply)
	}()

	kv.mu.Lock()
	if kv.isDuplicatedCommand(args.ClerkId, args.CommandId) {
		kv.mu.Unlock()
		reply.Err = ErrDoneCommandId
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:      CmdGet,
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notify := CommandNotify{
		clerkId:   args.ClerkId,
		commandId: args.CommandId,
		notifyCh:  make(chan *ApplyResult, 1),
	}
	kv.mu.Lock()
	kv.notifies[index] = notify
	kv.mu.Unlock()

	select {
	case result := <-notify.notifyCh:
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifies, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("{Node %v} receives request %v", kv.me, args)
	defer func() {
		DPrintf("{Node %v} receives request %v and reply %v", kv.me, args, reply)
	}()

	kv.mu.Lock()
	if kv.isDuplicatedCommand(args.ClerkId, args.CommandId) {
		lastAppliedResult := kv.CommandIds[args.ClerkId]
		kv.mu.Unlock()
		if args.CommandId == lastAppliedResult.MaxAppliedCommandId {
			reply.Err = lastAppliedResult.Result.Err
		} else {
			reply.Err = ErrDoneCommandId
		}
		return
	}
	kv.mu.Unlock()

	var opType CmdType
	switch args.Op {
	case "CmdPut":
		opType = CmdPut
	case "CmdAppend":
		opType = CmdAppend
	default:
		fmt.Errorf("unknown op type %v in %v", args.Op, args)
	}
	op := Op{
		Type:      opType,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	index, _, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	result := CommandNotify{
		clerkId:   args.ClerkId,
		commandId: args.CommandId,
		notifyCh:  make(chan *ApplyResult, 1),
	}
	kv.mu.Lock()
	kv.notifies[index] = result
	kv.mu.Unlock()

	select {
	case applyResult := <-result.notifyCh:
		reply.Err = applyResult.Err
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifies, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicatedCommand(clerkId int64, commandId uint64) bool {
	lastAppliedCommand, has := kv.CommandIds[clerkId]
	return has && commandId <= lastAppliedCommand.MaxAppliedCommandId
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		msg := <-kv.applyCh

		if msg.CommandValid {
			cmdIdx := msg.CommandIndex
			cmd := msg.Command.(Cmd)
			result := new(ApplyResult)
			kv.mu.Lock()
			// Todo: move cmd.Type's logic to struct StateMachine
			if kv.isDuplicatedCommand(cmd.ClerkId, cmd.CmdId) {
				//if cmd.Type == CmdGet {
				//	result.Err = ErrDoneCommandId
				//} else {
				//	lastAppliedResult := kv.CommandIds[cmd.ClerkId]
				//	if cmd.CmdId == lastAppliedResult.MaxAppliedCommandId {
				//		result.Err = lastAppliedResult.Result.Err
				//	} else {
				//		result.Err = ErrDoneCommandId
				//	}
				//}
				DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because"+
					" maxAppliedCommandId is %v for Clerk %v", kv.me, msg, kv.CommandIds[cmd.ClerkId], cmd.ClerkId)
				kv.mu.Unlock()
				continue
			} else {
				switch cmd.Type {
				case CmdGet:
					val, hasKey := kv.Data[cmd.Key]
					if hasKey {
						result.Err, result.Value = OK, val
					} else {
						result.Err, result.Value = ErrNoKey, ""
					}
				case CmdPut:
					kv.Data[cmd.Key] = cmd.Value
					result.Err = OK
				case CmdAppend:
					kv.Data[cmd.Key] += cmd.Value
					result.Err = OK
				}
				//DPrintf("{Node %v} applies command %v in message %v because maxAppliedCommandId is %v for Clerk %v",
				//	kv.me, cmd, msg, kv.CommandIds[cmd.ClerkId].MaxAppliedCommandId, cmd.ClerkId)
				kv.CommandIds[cmd.ClerkId] = CommandAppliedRecord{MaxAppliedCommandId: cmd.CmdId, Result: result}
			}
			// todo: consider following implement
			// if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
			//     ch := kv.getNotifyChan(message.CommandIndex)
			//	   ch <- response
			// }
			notifyResult, hasSub := kv.notifies[cmdIdx]
			if hasSub {
				if cmd.ClerkId == notifyResult.clerkId && cmd.CmdId == notifyResult.commandId {
					notifyResult.notifyCh <- result
				}
			}
			curStateSize := kv.persister.RaftStateSize()
			if kv.maxRaftState > 0 && curStateSize >= kv.maxRaftState {
				//DPrintf("{Node %v} starts to take Snapshot as RaftStateSize %v exceeds threshold %v", kv.me,
				//	curStateSize, kv.maxRaftState)
				kv.takeSnapshot(msg.CommandIndex)
				afterSize := kv.persister.RaftStateSize()
				if afterSize > 8*kv.maxRaftState {
					DPrintf("{Node %v} finishes taking Snapshot, size decrease %v, but RaftStateSize %v still "+
						"exceed 8x threshold %v", kv.me, curStateSize-afterSize, afterSize, kv.maxRaftState*8)
				}

			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.mu.Lock()
				kv.restoreSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
		} else {
			panic(fmt.Sprintf("{Node %v} apply unexpected Message %v", kv.me, msg))
		}

	}
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	e.Encode(kv.CommandIds)
	//DPrintf("takeSnapshot data %v commandIds %v", kv.Data, kv.CommandIds)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	data := make(map[string]string)
	commandIds := make(map[int64]CommandAppliedRecord)
	if d.Decode(&data) != nil || d.Decode(&commandIds) != nil {
		DPrintf("{Node %v} restores snapshot failed", kv.me)
		return
	}
	kv.Data, kv.CommandIds = data, commandIds
	DPrintf("{Node %v} restores snapshot succeed", kv.me)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Cmd{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(CmdArgs{})
	labgob.Register(CmdReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.Data = make(map[string]string)
	kv.CommandIds = make(map[int64]CommandAppliedRecord)
	kv.notifies = make(map[int]CommandNotify)
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.restoreSnapshot(persister.ReadSnapshot())
	go kv.applier()

	DPrintf("{Node %v} begins to start, maxRaftState %v", kv.me, kv.maxRaftState)

	return kv
}
