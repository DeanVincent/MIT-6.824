package raft

import (
	"sort"
	"time"
)

type AppendEntriesArgs struct {
	Term         int        // leader’s Term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	XGap    int  // last index of Follower's log
	XTerm   int  // the term of the conflicting entry
	XIndex  int  // the first index it stores for XTerm
}

type InstallSnapshotArgs struct {
	Term             int    // leader's term
	LeaderId         int    // so follower can redirect clients
	LastIncludeIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludeTerm  int    // term of lastIncludedIndex
	Data             []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) scheduleHeartbeat() {
	for !rf.killed() {
		rf.heartbeatCond.L.Lock()
		for rf.role != Leader {
			rf.heartbeatCond.Wait()
		}
		rf.heartbeatCond.L.Unlock()

		rf.BroadcastHeartbeat(true)
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		if isHeartbeat {
			go rf.replicateOneRound(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) sendHeartbeat(peer int) {
	rf.mu.Lock()
	args := rf.genAppendEntriesArgs(peer, true)
	reply := new(AppendEntriesReply)
	rf.mu.Unlock()
	if rf.sendAppendEntries(peer, args, reply) {
		rf.mu.Lock()
		rf.handleAppendEntriesReply(peer, args, reply)
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.lock("needReplicating")
	defer rf.unlock("needReplicating")
	return rf.role == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.lock("replicateOneRound")
	if rf.role != Leader {
		rf.unlock("replicateOneRound")
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1

	if prevLogIndex < rf.getFirstLog().Index {
		args := rf.genInstallSnapshotArgs()
		rf.unlock("replicateOneRound")
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.lock("replicateOneRound")
			defer rf.unlock("replicateOneRound")
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.persist()
				rf.ChangeRole(Follower)
			} else {
				rf.updateMatchIndex(peer, args.LastIncludeIndex)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				DPrintf("peer %d update matchIndex[%d]=%d nextIndex[%d]=%d\n", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
			}
		}
	} else {
		args := rf.genAppendEntriesArgs(peer, false)
		rf.unlock("replicateOneRound")
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.lock("handleAppendEntriesReply")
			rf.handleAppendEntriesReply(peer, args, reply)
			rf.unlock("handleAppendEntriesReply")
		}
	}
}

func (rf *Raft) genAppendEntriesArgs(peer int, isHeartbeat bool) *AppendEntriesArgs {
	prevLogIndex, prevLogTerm, entries := rf.getAppendEntries(peer, isHeartbeat)

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.getFirstLog().Index,
		LastIncludeTerm:  rf.getFirstLog().Term,
		Data:             rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) getAppendEntries(peer int, isHeartbeat bool) (prevLogIndex, prevLogTerm int, entries []LogEntry) {
	prevLog := rf.log[rf.getRelativeIndex(rf.nextIndex[peer]-1)]
	prevLogIndex = prevLog.Index
	prevLogTerm = prevLog.Term

	// heartbeat or probe
	if isHeartbeat || rf.matchIndex[peer] < rf.nextIndex[peer]-1 {
		entries = nil
		return
	}

	entries = append([]LogEntry{}, rf.log[rf.getRelativeIndex(rf.nextIndex[peer]):]...)
	return
}

// handleAppendEntriesReply
// consider reordering: send req1 -> send req2 -> recv res2 -> recv res1
func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}

	// discard reordering reply
	if args.PrevLogIndex < rf.matchIndex[peer] {
		DPrintf("{Node %v} receives reordering AppendEntriesRPC reply from {Node %v}, "+
			"prevLogIndex %v < matchIndex %v, args %#v, reply %#v\n", rf.me, peer, args.PrevLogIndex, rf.matchIndex,
			args, reply)
		return
	}

	if reply.Success {
		rf.updateMatchIndex(peer, args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		DPrintf("peer %d update matchIndex[%d]=%d nextIndex[%d]=%d\n", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
	} else {
		if reply.Term > rf.currentTerm {
			DPrintf("peer %v found newer term %v from peer %v than itself %v, from Leader to Follower\n", rf.me,
				reply.Term, peer, rf.currentTerm)
			rf.currentTerm, rf.voteFor = reply.Term, -1
			rf.persist()
			rf.ChangeRole(Follower)
			return
		} else if reply.Term == rf.currentTerm {
			if reply.XGap > 0 {
				rf.nextIndex[peer] = args.PrevLogIndex - reply.XGap + 1
				DPrintf("peer %d update nextIndex[%d]=%d\n", rf.me, peer, rf.nextIndex[peer])
			} else if rf.getLog(reply.XIndex).Term == reply.XTerm {
				rf.updateMatchIndex(peer, rf.findLastLogIndexOfTerm(reply.XTerm, reply.XIndex, args.PrevLogIndex).Index)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				DPrintf("peer %d update nextIndex[%d]=%d\n", rf.me, peer, rf.nextIndex[peer])
			} else {
				rf.nextIndex[peer] = reply.XIndex
				DPrintf("peer %d update nextIndex[%d]=%d\n", rf.me, peer, rf.nextIndex[peer])
			}
		}
	}
}

// updateMatchIndex
// keep matchIndex increasing monotonically
func (rf *Raft) updateMatchIndex(peer, matchIndex int) {
	// keep matchIndex increasing monotonically
	if rf.matchIndex[peer] >= matchIndex {
		return
	}
	rf.matchIndex[peer] = matchIndex

	if matchIndex > rf.commitIndex {
		rf.tryToUpdateLeadersCommitIndex()
	}
}

func (rf *Raft) tryToUpdateLeadersCommitIndex() {
	sortedMatchedIndex := make([]int, len(rf.peers))
	copy(sortedMatchedIndex, rf.matchIndex)
	sortedMatchedIndex[rf.me] = rf.getLastLog().Index
	sort.Ints(sortedMatchedIndex)
	// the log on which index is small or equal than len / 2 has been replicated on majority of cluster
	for i := len(sortedMatchedIndex) / 2; i >= 0 && sortedMatchedIndex[i] > rf.commitIndex; i-- {
		if rf.getLog(sortedMatchedIndex[i]).Term == rf.currentTerm {
			rf.commitIndex = sortedMatchedIndex[i]
			DPrintf("peer %v update commitIndex to %v\n", rf.me, rf.commitIndex)
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("peer %d send AppendEntriesRPC request %v to peer %d\n", rf.me, args, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//if ok {
	//	DPrintf("peer %d recv reply %#v from peer %d, which args %#v\n", rf.me, reply, server, args)
	//} else {
	//	DPrintf("peer %d don't recv AppendEntriesRPC reply from peer %d, which args %#v\n", rf.me, server, args)
	//}
	return ok
}

func (rf *Raft) sendInstallSnapshot(receiver int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("{node %v} send InstallSnapshotRPC request %v to {node %v}\n", rf.me, args, receiver)
	ok := rf.peers[receiver].Call("Raft.InstallSnapshot", args, reply)
	//if ok {
	//	DPrintf("{node %v} recv %#v from {node %v}, which args %#v\n", rf.me, reply, receiver, args)
	//} else {
	//	DPrintf("{node %v} don't recv InstallSnapshotReply from {node %v}, which args %#v\n", rf.me, receiver, args)
	//}
	return ok
}

// AppendEntries
// 1. Reply false if Term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at PrevLogIndex whose Term matches PrevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all
//that follow it (§5.3)
// 4. Append any new Entries not already in the log
// 5. If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")
	defer DPrintf("{Node %d}'s state is {Term %v, commitIndex %v, lastApplied %v, firstLog %v, lastLog %v} "+
		"before processing AppendEntriesArgs %v and reply AppendEntriesReply %v\n",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	if args.Term < rf.currentTerm {
		reply.Success, reply.Term = false, rf.currentTerm
		DPrintf("peer %d refuse to AppendEntriesRPC from peer %d, because the leader's term was smaller than "+
			"his current term\n", rf.me, args.LeaderId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.persist()
		DPrintf("peer %d update term to %v\n", rf.me, rf.currentTerm)
	}

	ProduceEventToChannel(rf.resetTimerEventCh)
	if rf.role == Candidate {
		ProduceEventToChannel(rf.stopElectionEventCh)
	} else {
		rf.ChangeRole(Follower)
	}

	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Success = false
		reply.Term = 0
		DPrintf("{Node %v} refuse to AppendEntriesRPC from {Node %v}, because prevLogIndex < firstLogIndex\n",
			rf.me, args.LeaderId)
		return
	}

	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Success, reply.Term = false, rf.currentTerm
		if lastIndex := rf.getLastLog().Index; lastIndex < args.PrevLogIndex {
			reply.XGap, reply.XTerm, reply.XIndex = args.PrevLogIndex-lastIndex, -1, -1
			DPrintf("peer %d refuse to AppendEntriesRPC from peer %d, because it doesn't contain an entry at %d\n",
				rf.me, args.LeaderId, args.PrevLogIndex)
		} else {
			prevLog := rf.log[rf.getRelativeIndex(args.PrevLogIndex)]
			XLog := rf.findFirstLogForTerm(prevLog.Term, rf.getFirstLog().Index, prevLog.Index)
			reply.XGap, reply.XTerm, reply.XIndex = 0, XLog.Term, XLog.Index
			DPrintf("peer %d refuse to AppendEntriesRPC from peer %d, because peer %d prev index %d term %d don't "+
				"match to term %d\n", rf.me, args.LeaderId, rf.me, args.PrevLogIndex, reply.XTerm, args.PrevLogTerm)
		}
		return
	}

	DPrintf("peer %d agrees to AppendEntriesRPC from peer %d\n", rf.me, args.LeaderId)

	reply.Success = true
	reply.Term = rf.currentTerm

	for index, entry := range args.Entries {
		relativeIndex := rf.getRelativeIndex(entry.Index)
		if relativeIndex >= len(rf.log) || entry.Term != rf.log[relativeIndex].Term {
			rf.log = append(rf.log[:relativeIndex], args.Entries[index:]...)
			rf.persist()
			DPrintf("peer %d update log to %v\n", rf.me, rf.log)
			break
		}
	}

	// 这句话不能使用 updateCommit := min(args.LeaderCommit, rf.getLastLog().Index)
	// 因为本地日志与 leader 一致的部分只到 args.PrevLogIndex+len(args.Entries)
	if updateCommit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)); updateCommit > rf.commitIndex {
		rf.commitIndex = updateCommit
		DPrintf("peer %d update commitIndex to %v\n", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
}

//func (rf *Raft) appendEntriesToLocal(entries []LogEntry) (n int, err error) {
//	m, ok := rf.tryGrowByReslice(len(entries))
//	if !ok {
//		m = rf.grow(len(entries))
//	}
//	return copy(rf.log[m:], entries), nil
//}
//
//func (rf *Raft) tryGrowByReslice(n int) (int, bool) {
//	if l := len(rf.log); n < cap(rf.log)-l {
//		rf.log = rf.log[:l+n]
//		return l, true
//	}
//	return 0, false
//}

// InstallSnapshot
// 1. Reply immediately if term < currentTerm
// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
// 7. Discard the entire log
// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unlock("InstallSnapshot")
	defer DPrintf("{Node %d}'s state is {Term %v, commitIndex %v, lastApplied %v, firstLog %v, lastLog %v} "+
		"before processing InstallSnapshotRPC args {Term %v LeaderId %v LastIncludeIndex %v LastIncludeTerm %v} and"+
		" reply {Term %v}",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args.Term,
		args.LeaderId, args.LastIncludeIndex, args.LastIncludeTerm, reply.Term)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
		rf.ChangeRole(Follower)
	}

	// 过期的 Snapshot, 条件同 CondInstallSnapshot
	if args.LastIncludeIndex <= rf.commitIndex {
		return
	}

	go func(args *InstallSnapshotArgs) {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludeTerm,
			SnapshotIndex: args.LastIncludeIndex,
		}
	}(args)
}

func (rf *Raft) matchLog(term, index int) bool {
	index = rf.getRelativeIndex(index)
	return index >= 0 && index < len(rf.log) && rf.log[index].Term == term
}

func (rf *Raft) findFirstLogForTerm0(term, l, r int) LogEntry {
	if rf.log[l].Term > term || rf.log[r].Term < term {
		return LogEntry{Index: -1}
	}

	for l < r-1 && rf.log[l].Term < term {
		m := (l + r) / 2
		if rf.log[m].Term < term {
			l = m + 1
		} else if rf.log[m].Term > term {
			r = m - 1
		} else {
			r = m
		}
	}

	if l == r-1 && rf.log[l].Term < term {
		return rf.log[r]
	}
	return rf.log[l]
}

func (rf *Raft) findFirstLogForTerm(term, l, r int) LogEntry {
	l = rf.getRelativeIndex(l)
	r = rf.getRelativeIndex(r)

	for l <= r && rf.log[l].Term <= term && rf.log[r].Term >= term {
		m := (l + r) / 2

		if rf.log[m].Term == term {
			if m == l || rf.log[m-1].Term < term {
				return rf.log[m]
			} else {
				r = m
			}
		} else if rf.log[m].Term < term {
			l = m + 1
		} else {
			r = m - 1
		}
	}

	return LogEntry{Index: -1}
}

func (rf *Raft) findLastLogIndexOfTerm0(term, i, j int) LogEntry {
	i = rf.getRelativeIndex(i)
	j = rf.getRelativeIndex(j)

	if rf.log[i].Term > term || rf.log[j].Term < term {
		return LogEntry{Index: -1}
	}

	for i < j-1 && rf.log[i].Term <= term && rf.log[j].Term > term {
		m := (i + j) / 2
		if rf.log[m].Term < term {
			i = m + 1
		} else if rf.log[m].Term > term {
			j = m - 1
		} else {
			i = m
		}
	}

	if rf.log[j].Term == term {
		return rf.log[j]
	} else if rf.log[j].Term > term && rf.log[i].Term == term {
		return rf.log[i]
	} else {
		return LogEntry{Index: -1}
	}
}

func (rf *Raft) findLastLogIndexOfTerm(term, l, r int) LogEntry {
	l = rf.getRelativeIndex(l)
	r = rf.getRelativeIndex(r)

	for i := 0; l <= r && rf.log[l].Term <= term && rf.log[r].Term >= term; i++ {
		m := (l + r) / 2
		//DPrintf("peer %d func findLastLogIndexOfTerm i[%d] l[%d] r[%d] m[%d]\n", rf.me, i, l, r, m)
		if rf.log[m].Term == term {
			if m == r || rf.log[m+1].Term > term {
				return rf.log[m]
			} else if m == l {
				return rf.log[r]
			} else {
				l = m
			}
		} else if rf.log[m].Term < term {
			l = m + 1
		} else {
			r = m - 1
		}
	}

	return LogEntry{Index: -1}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
