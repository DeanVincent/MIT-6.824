package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	currentConfig *shardctrler.Config
	lastConfig    *shardctrler.Config
	shards        map[int]*Shard                 // [shard id] -> *Shard
	lastApplied   map[int64]*OpReqWithResp       // [ClerkId] -> *OpReqWithResp
	notices       map[NoticeKey]chan interface{} // [cmd index in Raft log] -> Notice

	dead int32 // set by Kill()
}

/****************************** StateMachine Operations ******************************/

func (kv *ShardKV) execOp(req *OpRequest, resp *SMResponse) {
	shardId := key2shard(req.Key)
	shard := kv.shards[shardId]
	if kv.isDupOp(req) {
		DPrintf("{Node %v}{Group %v} doesn't apply duplicated OpRequest %v"+
			" because last applied operation is %v for Clerk %v", kv.me, kv.gid, *req,
			*kv.lastApplied[req.ClerkId].Req, req.ClerkId)
		resp.Err, resp.Value = kv.lastApplied[req.ClerkId].Resp.Err, ""
		return
	}
	if !kv.canServe(shardId) {
		resp.Err = ErrWrongGroup
		return
	}
	switch req.Type {
	case OpGet:
		if value, hasKey := shard.KVs[req.Key]; hasKey {
			resp.Err, resp.Value = OK, value
		} else {
			resp.Err, resp.Value = ErrNoKey, ""
		}
	case OpPut:
		shard.KVs[req.Key] = req.Value
		resp.Err = OK
	case OpAppend:
		shard.KVs[req.Key] += req.Value
		resp.Err = OK
	}
	kv.lastApplied[req.ClerkId] = &OpReqWithResp{Req: req, Resp: resp}
	DPrintf("{Node %v}{Group %v}'s state is %v after execute OpRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
}

func (kv *ShardKV) execPullShard(req *PullShardRequest, resp *SMResponse) {
	if req.ConfigNum != kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v} have duplicated PullShardRequest %v when currentConfig is %v",
			kv.me, kv.gid, *req, *kv.currentConfig)
		resp.Err = ErrWrongConfigNum
		return
	}
	DPrintf("{Node %v}{Group %v}'s state is %v before execute PullShardRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
	shard := kv.shards[req.ShardId]
	if shard.State != BeingPulled {
		resp.Err = ErrWrongShardState
		return
	}
	lastApplied := map[int64]*OpReqWithResp{}
	for k, v := range kv.lastApplied {
		if key2shard(v.Req.Key) == req.ShardId {
			lastApplied[k] = v
		}
	}
	resp.Err, resp.KVs, resp.LastApplied = OK, shard.deepCopy(), lastApplied
	DPrintf("{Node %v}{Group %v}'s state is %v after execute PullShardRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
}

func (kv *ShardKV) execInsertShard(req *InsertShardRequest, resp *SMResponse) {
	if req.ConfigNum != kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v} have duplicated InsertShardRequest %v when currentConfig is %v",
			kv.me, kv.gid, *req, *kv.currentConfig)
		resp.Err = ErrWrongConfigNum
		return
	}
	DPrintf("{Node %v}{Group %v}'s state is %v before execute InsertShardRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
	shard := kv.shards[req.ShardId]
	if shard.State != Pulling {
		resp.Err = ErrWrongShardState
		return
	}
	shard.State, shard.KVs = NotifyingGC, req.KVs
	for k, remote := range req.LastApplied {
		if local, isSaved := kv.lastApplied[k]; !isSaved || remote.Req.CmdId > local.Req.CmdId {
			kv.lastApplied[k] = remote
		}
	}
	DPrintf("{Node %v}{Group %v}'s state is %v after execute InsertShardRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
}

func (kv *ShardKV) execDeleteShard(req *DeleteShardRequest, resp *SMResponse) {
	if req.ConfigNum != kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v} have duplicated DeleteShardRequest %v when currentConfig is %v",
			kv.me, kv.gid, *req, *kv.currentConfig)
		resp.Err = OK
		return
	}
	DPrintf("{Node %v}{Group %v}'s state is %v before execute DeleteShardRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
	if shard := kv.shards[req.ShardId]; shard.State == BeingPulled {
		kv.shards[req.ShardId] = newShard()
	} else if shard.State == NotifyingGC {
		shard.State = Serving
	}
	resp.Err = OK
	DPrintf("{Node %v}{Group %v}'s state is %v after execute DeleteShardRequest %v", kv.me, kv.gid,
		kv.getStateString(), *req)
}

func (kv *ShardKV) execUpdateConfig(req *UpdateConfigRequest, resp *SMResponse) {
	if req.Config.Num != kv.currentConfig.Num+1 {
		DPrintf("{Node %v}{Group %v} refuse to execute WrongConfigNum UpdateConfigRequest %v when currentConfig is %v",
			kv.me, kv.gid, *req, *kv.currentConfig)
		resp.Err = ErrWrongConfigNum
		return
	}
	for shardId, shard := range kv.shards {
		if shard.State != Serving {
			DPrintf("{Node %v}{Group %v} refuse to execute UpdateConfigRequest %v when shard %v's state is %v",
				kv.me, kv.gid, *req, shardId, shard.State)
			resp.Err = ErrWrongShardState
			return
		}
	}
	DPrintf("{Node %v}{Group %v}'s state is %v before update currentConfig to %v", kv.me, kv.gid,
		kv.getStateString(), req.Config)
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = req.Config
	for shardId := 0; shardId < len(kv.shards); shardId++ {
		if kv.lastConfig.Shards[shardId] == kv.gid && kv.currentConfig.Shards[shardId] != kv.gid {
			kv.shards[shardId].State = BeingPulled
		} else if kv.lastConfig.Shards[shardId] != kv.gid && kv.currentConfig.Shards[shardId] == kv.gid {
			if kv.lastConfig.Shards[shardId] == shardctrler.NoGroup {
				kv.shards[shardId].State = Serving
			} else {
				kv.shards[shardId].State = Pulling
			}
		}
	}
	DPrintf("{Node %v}{Group %v}'s state is %v after update currentConfig to %v", kv.me, kv.gid,
		kv.getStateString(), req.Config)
}

/****************************** RPC Handlers ******************************/

// 任何 RPC Handler 都要考虑重试的情况

func (kv *ShardKV) Op(req *OpRequest, resp *OpResponse) {
	kv.mu.Lock()
	DPrintf("{Node %v}{Group %v} receives OpRequest %v, key %v -> shardId %v, kv.state %v", kv.me, kv.gid, *req,
		req.Key, key2shard(req.Key), kv.getStateString())
	if kv.isDupOp(req) {
		opReqWithResp := kv.lastApplied[req.ClerkId]
		//if req.Type == OpGet {
		//	resp.Err, resp.Value = ErrWrongCmdId, ""
		//} else {
		//	resp.Err, resp.Value = opReqWithResp.Err, ""
		//}
		resp.Err, resp.Value = opReqWithResp.Resp.Err, opReqWithResp.Resp.Value
		kv.mu.Unlock()
		return
	}
	if !kv.canServe(key2shard(req.Key)) {
		resp.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	cmdResp := kv.propose(&SMRequest{RequestType: ReqOp, Op: req})
	resp.Err, resp.Value = cmdResp.Err, cmdResp.Value
	DPrintf("{Node %v}{Group %v} complete executing OpRequest %v, return OpResponse %v", kv.me, kv.gid, *req, *resp)
}

const ExecuteTimeout = 500 * time.Millisecond

func (kv *ShardKV) PullShard(req *PullShardRequest, resp *PullShardResponse) {
	kv.mu.Lock()
	DPrintf("{Node %v}{Group %v} receives PullShardRequest %v", kv.me, kv.gid, *req)
	if req.ConfigNum != kv.currentConfig.Num {
		kv.mu.Unlock()
		resp.Err = ErrWrongConfigNum
		return
	}
	if kv.shards[req.ShardId].State != BeingPulled {
		kv.mu.Unlock()
		resp.Err = ErrWrongShardState
		return
	}
	kv.mu.Unlock()
	cmdResp := kv.propose(&SMRequest{RequestType: ReqPullShard, PullShard: req})
	resp.Err, resp.KVs, resp.LastApplied = cmdResp.Err, cmdResp.KVs, cmdResp.LastApplied
	DPrintf("{Node %v}{Group %v} complete executing PullShardRequest %v, return PullShardResponse %v", kv.me,
		kv.gid, *req, *resp)
}

func (kv *ShardKV) DeleteShard(req *DeleteShardRequest, resp *DeleteShardResponse) {
	kv.mu.Lock()
	DPrintf("{Node %v}{Group %v} receives DeleteShardRequest %v", kv.me, kv.gid, *req)
	if req.ConfigNum < kv.currentConfig.Num {
		kv.mu.Unlock()
		resp.Err = OK
		return
	}
	if kv.shards[req.ShardId].State != BeingPulled {
		kv.mu.Unlock()
		resp.Err = ErrWrongShardState
		return
	}
	kv.mu.Unlock()
	cmdResp := kv.propose(&SMRequest{RequestType: ReqDeleteShard, DeleteShard: req})
	resp.Err = cmdResp.Err
	DPrintf("{Node %v}{Group %v} complete executing DeleteShardRequest %v, return DeleteShardResponse %v", kv.me,
		kv.gid, *req, *resp)
}

/****************************** Threads ******************************/

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			raftReq := new(SMRequest)
			dAtA := msg.Command.([]byte)
			raftReq.Unmarshal(dAtA)
			resp := new(SMResponse)
			switch raftReq.RequestType {
			case ReqOp:
				req := raftReq.Op
				kv.execOp(req, resp)
			case ReqPullShard:
				req := raftReq.PullShard
				kv.execPullShard(req, resp)
			case ReqInsertShard:
				req := raftReq.InsertShard
				kv.execInsertShard(req, resp)
			case ReqDeleteShard:
				req := raftReq.DeleteShard
				kv.execDeleteShard(req, resp)
			case ReqUpdateConfig:
				req := raftReq.UpdateConfig
				kv.execUpdateConfig(req, resp)
			}
			kv.maybeNotifyResult(msg.CommandIndex, msg.CommandTerm, resp)

			if kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				kv.takeSnapshot(msg.CommandIndex)
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

func (kv *ShardKV) monitor(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) updateConfigAction() {
	DPrintf("{Node %v}{Group %v} start updateConfigAction", kv.me, kv.gid)
	ck := shardctrler.MakeClerk(kv.ctrlers)
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.State != Serving {
			kv.mu.Unlock()
			return
		}
	}
	expectConfigNum := kv.currentConfig.Num + 1
	kv.mu.Unlock()
	newConfig := ck.Query(expectConfigNum)
	if newConfig.Num != expectConfigNum {
		return
	}
	kv.propose(&SMRequest{
		RequestType: ReqUpdateConfig,
		UpdateConfig: &UpdateConfigRequest{
			Config: &newConfig,
		}})
	DPrintf("{Node %v}{Group %v} finish updateConfigAction", kv.me, kv.gid)
}

func (kv *ShardKV) pullShardAction() {
	DPrintf("{Node %v}{Group %v} start pullShardAction", kv.me, kv.gid)
	kv.mu.Lock()
	shardIds := kv.getShardIdsWithState(Pulling)
	var wg sync.WaitGroup
	for _, shardId := range shardIds {
		wg.Add(1)
		gid := kv.lastConfig.Shards[shardId]
		servers := kv.lastConfig.Groups[gid]
		go func(configNum, shardId int, servers []string) {
			defer wg.Done()
			req := &PullShardRequest{ConfigNum: configNum, ShardId: shardId}
			resp := new(PullShardResponse)
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.PullShard", req, resp)
				DPrintf("{Node %v}{Group %v} send PullShardRequest %v to server %v and receives %v "+
					"PullShardResponse %v", kv.me, kv.gid, *req, srv, ok, *resp)
				if ok && resp.Err == OK {
					kv.propose(&SMRequest{
						RequestType: ReqInsertShard,
						InsertShard: &InsertShardRequest{
							ConfigNum:   configNum,
							ShardId:     shardId,
							KVs:         resp.KVs,
							LastApplied: resp.LastApplied,
						}})
					DPrintf("{Node %v}{Group %v} pullShardAction shardId %v at here 1", kv.me, kv.gid, shardId)
					return
				}
			}
		}(kv.currentConfig.Num, shardId, servers)
	}
	kv.mu.Unlock()
	DPrintf("{Node %v}{Group %v} pullShardAction at here", kv.me, kv.gid)
	wg.Wait()
	DPrintf("{Node %v}{Group %v} finish pullShardAction", kv.me, kv.gid)
}

func (kv *ShardKV) deleteShardAction() {
	DPrintf("{Node %v}{Group %v} start deleteShardAction", kv.me, kv.gid)
	kv.mu.Lock()
	shardIds := kv.getShardIdsWithState(NotifyingGC)
	var wg sync.WaitGroup
	for _, shardId := range shardIds {
		wg.Add(1)
		gid := kv.lastConfig.Shards[shardId]
		servers := kv.lastConfig.Groups[gid]
		go func(configNum, shardId int, servers []string) {
			defer wg.Done()
			req := &DeleteShardRequest{ConfigNum: configNum, ShardId: shardId}
			resp := new(DeleteShardResponse)
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.DeleteShard", req, resp)
				DPrintf("{Node %v}{Group %v} send DeleteShardRequest %v to {Node %v}{Group %v} and receives %v "+
					"DeleteShardResponse %v", kv.me, kv.gid, *req, si, gid, ok, *resp)
				if ok && resp.Err == OK {
					kv.propose(&SMRequest{
						RequestType: ReqDeleteShard,
						DeleteShard: &DeleteShardRequest{
							ConfigNum: configNum,
							ShardId:   shardId,
						}})
					return
				}
			}
		}(kv.currentConfig.Num, shardId, servers)
	}
	kv.mu.Unlock()
	wg.Wait()
	DPrintf("{Node %v}{Group %v} finish deleteShardAction", kv.me, kv.gid)
}

func (kv *ShardKV) isDupOp(op *OpRequest) bool {
	opReqWithResp, hasApplied := kv.lastApplied[op.ClerkId]
	return hasApplied && op.CmdId <= opReqWithResp.Req.CmdId
}

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid &&
		(kv.shards[shardId].State == Serving || kv.shards[shardId].State == NotifyingGC)
}

func (kv *ShardKV) maybeNotifyResult(cmdIdx int, cmdTerm int, response interface{}) {
	if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == cmdTerm {
		// 在极少情况下, 此处的 get notice 可能先于 propose 中的 create notice, 所以不仅需要 get, 还需要 create
		notice := kv.getOrCreateNotice(cmdIdx, cmdTerm)
		notice <- response
		DPrintf("{Node %v}{Group %v} maybeNotifyResult cmdIdx %v has notified", kv.me, kv.gid, cmdIdx)
	} else {
		DPrintf("{Node %v}{Group %v} maybeNotifyResult cmdIdx %v have not notified", kv.me, kv.gid, cmdIdx)
	}
}

func (kv *ShardKV) getOrCreateNotice(cmdIdx int, cmdTerm int) chan interface{} {
	key := NoticeKey{Index: cmdIdx, Term: cmdTerm}
	notice, has := kv.notices[key]
	if !has {
		notice = make(chan interface{}, 1)
		kv.notices[key] = notice
	}
	return notice
}

func (kv *ShardKV) deleteNotice(cmdIdx int, cmdTerm int) {
	delete(kv.notices, NoticeKey{Index: cmdIdx, Term: cmdTerm})
}

func (kv *ShardKV) getShardIdsWithState(state ShardState) []int {
	var shardIds []int
	for i, shard := range kv.shards {
		if shard.State == state {
			shardIds = append(shardIds, i)
		}
	}
	return shardIds
}

func (kv *ShardKV) propose(req *SMRequest) *SMResponse {
	resp := new(SMResponse)
	dAtA := req.Marshal()
	idx, term, isLeader := kv.rf.Start(dAtA)
	if !isLeader {
		resp.Err = ErrWrongLeader
		return resp
	}
	DPrintf("{Node %v}{Group %v} Raft size %v after propose %v", kv.me, kv.gid, kv.rf.GetRaftStateSize(), len(dAtA))
	kv.mu.Lock()
	notice := kv.getOrCreateNotice(idx, term)
	kv.mu.Unlock()
	select {
	case msg := <-notice:
		resp = msg.(*SMResponse)
	case <-time.After(ExecuteTimeout):
		resp.Err = ErrTimeOut
	}
	go func() {
		kv.mu.Lock()
		kv.deleteNotice(idx, term)
		kv.mu.Unlock()
	}()
	return resp
}

func (kv *ShardKV) takeSnapshot(cmdIdx int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.currentConfig)
	e.Encode(kv.lastConfig)
	e.Encode(kv.shards)
	e.Encode(kv.lastApplied)
	kv.rf.Snapshot(cmdIdx, w.Bytes())
	DPrintf("{Node %v}{Group %v} snapshot size %v Raft size %v", kv.me, kv.gid, w.Len(), kv.rf.GetRaftStateSize())
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) { // todo
	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	currentConfig := shardctrler.NewDefaultConfig()
	lastConfig := shardctrler.NewDefaultConfig()
	shards := map[int]*Shard{}
	lastApplied := map[int64]*OpReqWithResp{}
	if dec.Decode(currentConfig) != nil ||
		dec.Decode(lastConfig) != nil ||
		dec.Decode(&shards) != nil ||
		dec.Decode(&lastApplied) != nil {
		DPrintf("{Node %v} restores snapshot failed", kv.me)
		return
	}
	kv.currentConfig, kv.lastConfig, kv.shards, kv.lastApplied = currentConfig, lastConfig, shards, lastApplied
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("{Node %v}{Group %v} has been killed", kv.me, kv.gid)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpRequest{})
	labgob.Register(OpResponse{})
	labgob.Register(PullShardRequest{})
	labgob.Register(PullShardResponse{})
	labgob.Register(InsertShardRequest{})
	labgob.Register(InsertShardResponse{})
	labgob.Register(DeleteShardRequest{})
	labgob.Register(DeleteShardResponse{})
	labgob.Register(UpdateConfigRequest{})
	labgob.Register(UpdateConfigResponse{})
	labgob.Register(SMRequest{})
	labgob.Register(SMResponse{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.currentConfig = shardctrler.NewDefaultConfig()
	kv.lastConfig = kv.currentConfig
	kv.shards = map[int]*Shard{}
	for shardId := 0; shardId < shardctrler.NShards; shardId++ {
		kv.shards[shardId] = newShard()
	}
	kv.lastApplied = map[int64]*OpReqWithResp{}
	kv.notices = map[NoticeKey]chan interface{}{}
	if persister.SnapshotSize() > 0 {
		kv.restoreSnapshot(persister.ReadSnapshot())
	}

	go kv.applier()
	go kv.monitor(kv.updateConfigAction, TimeoutUpdateConfig)
	go kv.monitor(kv.pullShardAction, TimeoutPullShard)
	go kv.monitor(kv.deleteShardAction, TimeoutDeleteShard)

	return kv
}

func (kv *ShardKV) getStateString() string {
	var states []string
	for id := 0; id < shardctrler.NShards; id++ {
		states = append(states, kv.shards[id].State.toString())
	}
	return fmt.Sprintf("{currentConfig %v, lastConfig %v, shardStates %v}", kv.currentConfig, kv.lastConfig, states)
}

type SMRequest struct {
	RequestType  RequestType
	Op           *OpRequest
	PullShard    *PullShardRequest
	InsertShard  *InsertShardRequest
	DeleteShard  *DeleteShardRequest
	UpdateConfig *UpdateConfigRequest
}

type RequestType uint8

const (
	ReqOp = iota
	ReqPullShard
	ReqInsertShard
	ReqDeleteShard
	ReqUpdateConfig
)

func (r *SMRequest) Marshal() []byte {
	b := new(bytes.Buffer)
	enc := labgob.NewEncoder(b)
	enc.Encode(r)
	return b.Bytes()
}

func (r *SMRequest) Unmarshal(dAtA []byte) {
	b := bytes.NewBuffer(dAtA)
	dec := labgob.NewDecoder(b)
	dec.Decode(r)
}

type SMResponse struct {
	Err         Err
	Value       string
	KVs         map[string]string
	LastApplied map[int64]*OpReqWithResp
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type OpType uint8

const (
	OpGet = iota
	OpPut
	OpAppend
)

type Shard struct {
	State ShardState
	KVs   map[string]string
}

func (s *Shard) deepCopy() map[string]string {
	copyKVs := map[string]string{}
	for k, v := range s.KVs {
		copyKVs[k] = v
	}
	return copyKVs
}

func newShard() *Shard {
	return &Shard{
		State: Serving,
		KVs:   map[string]string{},
	}
}

type ShardState uint8

const (
	Serving = iota
	Pulling
	BeingPulled
	NotifyingGC
)

func (s ShardState) toString() string {
	switch s {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BeingPulled:
		return "BeingPulled"
	case NotifyingGC:
		return "NotifyingGC"
	}
	return ""
}

type OpReqWithResp struct {
	Req  *OpRequest
	Resp *SMResponse
}

type NoticeKey struct {
	Index int
	Term  int
}

const (
	TimeoutUpdateConfig = 50 * time.Millisecond
	TimeoutPullShard    = 50 * time.Millisecond
	TimeoutDeleteShard  = 50 * time.Millisecond
)
