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
	notices      map[int]notice
}

type CmdResult struct {
	Id     int64 // for checking whether cmd is duplicated
	Err    Err
	Config *Config
}

type notice struct {
	clerkId int64
	cmdId   int64
	ch      chan *CmdResult
}

const ExecuteTimeout = 1000 * time.Millisecond

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

	idx, _, isLeader := sc.rf.Start(Cmd{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notice := notice{
		clerkId: args.ClerkId,
		cmdId:   args.CmdId,
		ch:      make(chan *CmdResult),
	}
	sc.mu.Lock()
	sc.notices[idx] = notice
	sc.mu.Unlock()

	select {
	case result := <-notice.ch:
		reply.Err = result.Err
		if result.Config != nil {
			reply.Config = *result.Config
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notices, idx)
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
			sc.maybeNotify(msg.CommandIndex, cmd, result)
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

func (sc *ShardCtrler) maybeNotify(idx int, cmd Cmd, result *CmdResult) {
	notice, has := sc.notices[idx]
	if has && cmd.ClerkId == notice.clerkId && cmd.CmdId == notice.cmdId {
		notice.ch <- result
	}
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
	sc.notices = make(map[int]notice)

	go sc.applier()

	return sc
}
