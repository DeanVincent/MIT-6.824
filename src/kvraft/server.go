// Q: Clerk 有时不知道哪个是 leader。如果 Clerk 向错误的 kvserver 发送RPC，或者无法到达kvserver，书记员应该通过向不同的kvserver发送来重新尝试。
// 或者 kvserver 可以将自己认为的 leader 返回给 Clerk

// 当 Raft 日志条目数量超过 maxRaftState 时, KVServer 命令 Raft 进行快照.
//
// 快照包含的数据有: Data, CommandIds
//
// Raft 要求 KVServer 安装快照

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

type CommandResult struct {
	clerkId   int64
	commandId uint64
	resultCh  chan *ApplyResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	// Your definitions here.

	Data       map[string]string     // state, kv pairs
	CommandIds map[int64]uint64      // state, latest commandId for each Clerk
	results    map[int]CommandResult // op and result for each log index

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("{Node %v} receives request %v", kv.me, args)
	defer func() {
		DPrintf("{Node %v} receives request %v and reply %v", kv.me, args, reply)
	}()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	latestCommandId, has := kv.CommandIds[args.ClerkId]
	kv.mu.Unlock()
	if has && latestCommandId >= args.CommandId {
		reply.Err = ErrDoneCommandId
		return
	}

	op := Op{
		Type:      GetOp,
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}
	index, _, success := kv.rf.Start(op)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	result := CommandResult{
		clerkId:   args.ClerkId,
		commandId: args.CommandId,
		resultCh:  make(chan *ApplyResult, 1),
	}
	kv.mu.Lock()
	kv.results[index] = result
	kv.mu.Unlock()

	select {
	case applyResult := <-result.resultCh:
		reply.Err, reply.Value = applyResult.Err, applyResult.Value
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.results, index)
		kv.mu.Unlock()
	}()

	//for {
	//	select {
	//	case applyResult := <-result.resultCh:
	//		reply.Err, reply.Value = applyResult.Err, applyResult.Value
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

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	commandId, has := kv.CommandIds[args.ClerkId]
	kv.mu.Unlock()
	if has && commandId >= args.CommandId {
		reply.Err = OK
		return
	}

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
	DPrintf("{Node %v} receives has submit request %v", kv.me, args)

	result := CommandResult{
		clerkId:   args.ClerkId,
		commandId: args.CommandId,
		resultCh:  make(chan *ApplyResult, 1),
	}
	kv.mu.Lock()
	kv.results[index] = result
	kv.mu.Unlock()

	select {
	case applyResult := <-result.resultCh:
		reply.Err = applyResult.Err
	case <-time.After(2 * time.Second):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.results, index)
		kv.mu.Unlock()
	}()

	//for {
	//	select {
	//	case applyResult := <-result.resultCh:
	//		reply.Err = applyResult.Err
	//		break
	//	case <-time.After(time.Second):
	//		if curTerm, _ := kv.rf.GetState(); curTerm > term {
	//			reply.Err = ErrWrongLeader
	//			break
	//		}
	//	}
	//}
}

func (kv *KVServer) isDuplicatedCommand(clerkId int64, commandId uint64) bool {
	appliedCommandId, has := kv.CommandIds[clerkId]
	return has && commandId <= appliedCommandId
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
				if op.Type == GetOp {
					result.Err = ErrDoneCommandId
				} else {
					// todo: RVServer should save lastApplyResult for each Clerk and should assign with the saved result
					result.Err = OK
				}
			} else if op.Type == GetOp {
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
			kv.CommandIds[op.ClerkId] = op.CommandId
			notifyResult, hasSub := kv.results[cmdIdx]
			kv.mu.Unlock()
			if hasSub {
				if op.ClerkId != notifyResult.clerkId || op.CommandId != notifyResult.commandId {
					result.Err = ErrWrongLeader
					result.Value = ""
				}
				notifyResult.resultCh <- result
			}
			DPrintf("{Node %v} applies command %v", kv.me, op)
			kv.mu.Lock()
			if kv.maxRaftState > 0 && kv.persister.RaftStateSize() >= kv.maxRaftState {
				// 进行快照
				snapshot := kv.encodeState()
				kv.mu.Unlock()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
				kv.mu.Lock()
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.mu.Lock()
				kv.installSnapshot(msg.Snapshot)
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
		//	result, hasSub := kv.results[cmdIdx]
		//	delete(kv.results, cmdIdx)
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
		//	result.resultCh <- applyResult
		//case PutOp:
		//	kv.mu.Lock()
		//	kv.Data[op.Key] = op.Value
		//	kv.CommandIds[op.ClerkId] = op.CommandId
		//	result, hasSub := kv.results[cmdIdx]
		//	delete(kv.results, cmdIdx)
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
		//	result.resultCh <- applyResult
		//case AppendOp:
		//	kv.mu.Lock()
		//	kv.Data[op.Key] += op.Value
		//	kv.CommandIds[op.ClerkId] = op.CommandId
		//	result, hasSub := kv.results[cmdIdx]
		//	delete(kv.results, cmdIdx)
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
		//	result.resultCh <- applyResult
		//}
	}
}

func (kv *KVServer) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Data)
	e.Encode(kv.CommandIds)
	return w.Bytes()
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.Data)
	d.Decode(&kv.CommandIds)
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
	kv.CommandIds = make(map[int64]uint64)
	kv.results = make(map[int]CommandResult)
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.installSnapshot(persister.ReadSnapshot())
	go kv.applier()

	DPrintf("{Node %v} begins to start, maxRaftState %v", kv.me, kv.maxRaftState)

	return kv
}
