// Q: Clerk 有时不知道哪个是 leader。如果 Clerk 向错误的 kvserver 发送RPC，或者无法到达kvserver，书记员应该通过向不同的kvserver发送来重新尝试。
// 或者 kvserver 可以将自己认为的 leader 返回给 Clerk

// 当 Raft 日志条目数量超过 maxRaftState 时, KVServer 命令 Raft 进行快照.
//
// 快照包含的数据有: Data, CommandIds
//
// Raft 要求 KVServer 安装快照
//
// 场景一: Raft apply Snapshot(index: y) -> Raft apply Command c1(index: x)
// 		  其中, x < y, 状态机应该识别 c1 是重复的, 通过 c1.ClerkId, c1.CommandId 与 kv.CommandIds相比较得出

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

type OpType uint8

const (
	GetOp OpType = iota
	PutOp
	AppendOp
)

type Op struct {
	Type      OpType
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
	CommandIds map[int64]CommandAppliedRecord // state, last commandId and result for each Clerk
	notifies   map[int]CommandNotify          // op and result for each log index

	persister *raft.Persister
}

type CommandAppliedRecord struct {
	MaxAppliedCommandId uint64       // commandId
	Result              *ApplyResult // command applied result
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
		Type:      GetOp,
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

	//for {
	//	select {
	//	case result := <-notify.notifyCh:
	//		reply.Err, reply.Value = result.Err, result.Value
	//		break
	//	case <-time.After(time.Second):
	//		if curTerm, _ := kv.rf.GetState(); curTerm > term {
	//			reply.Err = ErrWrongLeader
	//			break
	//		}
	//	}
	//}
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

	var opType OpType
	switch args.Op {
	case "Put":
		opType = PutOp
	case "Append":
		opType = AppendOp
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
			op := msg.Command.(Op)
			result := new(ApplyResult)
			kv.mu.Lock()
			// Todo: move op.Type's logic to struct StateMachine
			if kv.isDuplicatedCommand(op.ClerkId, op.CommandId) {
				//if op.Type == GetOp {
				//	result.Err = ErrDoneCommandId
				//} else {
				//	lastAppliedResult := kv.CommandIds[op.ClerkId]
				//	if op.CommandId == lastAppliedResult.MaxAppliedCommandId {
				//		result.Err = lastAppliedResult.Result.Err
				//	} else {
				//		result.Err = ErrDoneCommandId
				//	}
				//}
				DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because"+
					" maxAppliedCommandId is %v for Clerk %v", kv.me, msg, kv.CommandIds[op.ClerkId], op.ClerkId)
				kv.mu.Unlock()
				continue
			} else {
				if op.Type == GetOp {
					val, hasKey := kv.Data[op.Key]
					if hasKey {
						result.Err, result.Value = OK, val
					} else {
						result.Err, result.Value = ErrNoKey, ""
					}
				} else if op.Type == PutOp {
					kv.Data[op.Key] = op.Value
					result.Err = OK
				} else if op.Type == AppendOp {
					kv.Data[op.Key] += op.Value
					result.Err = OK
				}
				//DPrintf("{Node %v} applies command %v in message %v because maxAppliedCommandId is %v for Clerk %v",
				//	kv.me, op, msg, kv.CommandIds[op.ClerkId].MaxAppliedCommandId, op.ClerkId)
				kv.CommandIds[op.ClerkId] = CommandAppliedRecord{MaxAppliedCommandId: op.CommandId, Result: result}
			}
			// todo: consider following implement
			// if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
			//     ch := kv.getNotifyChan(message.CommandIndex)
			//	   ch <- response
			// }
			notifyResult, hasSub := kv.notifies[cmdIdx]
			if hasSub {
				if op.ClerkId == notifyResult.clerkId && op.CommandId == notifyResult.commandId {
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

		//switch op.Type {
		//case GetOp:
		//	kv.mu.Lock()
		//	val, hasKey := kv.Data[op.Key]
		//	kv.CommandIds[op.ClerkId] = op.CommandId
		//	result, hasSub := kv.notifies[cmdIdx]
		//	delete(kv.notifies, cmdIdx)
		//	kv.mu.Unlock()
		//	if !hasSub {
		//		continue
		//	}
		//	applyResult := new(ApplyResult)
		//	if op.ClerkId != result.clerkId || op.CommandId != result.commandId {
		//		applyResult.Err = ErrWrongLeader
		//		applyResult.Value = ""
		//	} else if !hasKey {
		//		applyResult.Err = ErrNoKey
		//		applyResult.Value = ""
		//	} else {
		//		applyResult.Err = OK
		//		applyResult.Value = val
		//	}
		//	result.notifyCh <- applyResult
		//case PutOp:
		//	kv.mu.Lock()
		//	kv.Data[op.Key] = op.Value
		//	kv.CommandIds[op.ClerkId] = op.CommandId
		//	result, hasSub := kv.notifies[cmdIdx]
		//	delete(kv.notifies, cmdIdx)
		//	kv.mu.Unlock()
		//	if !hasSub {
		//		continue
		//	}
		//	applyResult := new(ApplyResult)
		//	if op.ClerkId != result.clerkId || op.CommandId != result.commandId {
		//		applyResult.Err = ErrWrongLeader
		//	} else {
		//		applyResult.Err = OK
		//	}
		//	result.notifyCh <- applyResult
		//case AppendOp:
		//	kv.mu.Lock()
		//	kv.Data[op.Key] += op.Value
		//	kv.CommandIds[op.ClerkId] = op.CommandId
		//	result, hasSub := kv.notifies[cmdIdx]
		//	delete(kv.notifies, cmdIdx)
		//	kv.mu.Unlock()
		//	if !hasSub {
		//		continue
		//	}
		//	applyResult := new(ApplyResult)
		//	if op.ClerkId != result.clerkId || op.CommandId != result.commandId {
		//		applyResult.Err = ErrWrongLeader
		//	} else {
		//		applyResult.Err = OK
		//	}
		//	result.notifyCh <- applyResult
		//}
	}
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	e.Encode(kv.CommandIds)
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
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

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
