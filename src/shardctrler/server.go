package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	stateMachine ctrlerStateMachine
	history      map[int64]*CmdResult
	notifyChs    map[noticeKey]chan *CmdResult
}

type CmdResult struct {
	Id     int64 // for checking whether cmd is duplicated
	Err    Err
	Config *Config
}

type noticeKey struct {
	idx  int
	term int
}

const ExecuteTimeout = 500 * time.Millisecond

func (sc *ShardCtrler) Cmd(args *CmdArgs, reply *CmdReply) {
	DPrintf("{Node %v} start to handle request %v", sc.me, *args)
	defer func() {
		DPrintf("{Node %v} complete executing request %v and reply %v", sc.me, *args, *reply)
	}()

	sc.mu.Lock()
	if sc.isDupCmd(args.ClerkId, args.CmdId) {
		lastReply := sc.history[args.ClerkId]
		reply.Err = lastReply.Err
		if lastReply.Config != nil {
			reply.Config = *lastReply.Config
		}

		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	idx, term, isLeader := sc.rf.Start(Cmd{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	key := noticeKey{idx: idx, term: term}
	sc.mu.Lock()
	ch := sc.getOrCreateNotice(key)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
		if result.Config != nil {
			reply.Config = *result.Config
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.deleteNotice(key)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		msg := <-sc.applyCh
		if msg.CommandValid {
			cmd := msg.Command.(Cmd)
			sc.mu.Lock()
			if sc.isDupCmd(cmd.ClerkId, cmd.CmdId) {
				DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because"+
					" maxAppliedCommandId is %v for Clerk %v", sc.me, msg, sc.history[cmd.ClerkId], cmd.ClerkId)
				sc.mu.Unlock()
				continue
			}

			result := new(CmdResult)
			result.Id = cmd.CmdId
			result.Config, result.Err = sc.stateMachine.exec(cmd)
			DPrintf("{Node %v} applies command in message %v", sc.me, msg)
			sc.history[cmd.ClerkId] = result
			sc.maybeNotify(msg.CommandIndex, msg.CommandTerm, result)
			// no need to take snapshot
			sc.mu.Unlock()
		} else if msg.SnapshotValid {
			panic(fmt.Sprintf("{Node %v} applies unexpected Message %v", sc.me, msg))
		} else {
			panic(fmt.Sprintf("{Node %v} applies unexpected Message %v", sc.me, msg))
		}
	}
}

func (sc *ShardCtrler) isDupCmd(clerkId, cmdId int64) bool {
	appliedCmd, has := sc.history[clerkId]
	return has && cmdId <= appliedCmd.Id
}

func (sc *ShardCtrler) maybeNotify(cmdIdx int, cmdTerm int, result *CmdResult) {
	if currentTerm, isLeader := sc.rf.GetState(); isLeader && cmdTerm == currentTerm {
		ch := sc.getOrCreateNotice(noticeKey{idx: cmdIdx, term: cmdTerm})
		ch <- result
	}
}

func (sc *ShardCtrler) getOrCreateNotice(key noticeKey) chan *CmdResult {
	ch, hasKey := sc.notifyChs[key]
	if !hasKey {
		ch = make(chan *CmdResult, 1)
		sc.notifyChs[key] = ch
	}
	return ch
}

func (sc *ShardCtrler) deleteNotice(key noticeKey) {
	delete(sc.notifyChs, key)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Cmd{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.stateMachine = newMemoryCtrler(me)
	sc.history = make(map[int64]*CmdResult)
	sc.notifyChs = make(map[noticeKey]chan *CmdResult)

	go sc.applier()

	return sc
}
