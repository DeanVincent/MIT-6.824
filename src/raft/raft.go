package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{} // state machine command
	Term    int         // used to detect inconsistencies between logs and properies in Figure3
	Index   int         // identify this entry's position in the log
}

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

var nameMap = map[Role]string{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

const TimeoutMin int64 = 250 // measured in milliseconds
const TimeoutMax int64 = 500 // measured in milliseconds
const HeartbeatInterval time.Duration = 100 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers:
	currentTerm int        // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int        // CandidateId that received vote in current Term (or -1 if none)
	log         []LogEntry // l1 log Entries; each entry contains Command for state machine, and Term when entry was
	// l2 received by leader, the first element is LogEntry{Command: nil, Term: LastSnapshotTerm, Index: LastSnapshotIndex}

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders (Reinitialized after election):
	nextIndex []int // l1 for each server, index of the next log entry to send to that server
	// l2 (initialized to leader last log index + 1)
	matchIndex []int // l1 for each server, index of highest log entry known to be replicated on server
	// l2 (initialized to 0, increases monotonically)

	role Role // this peer's role
	// l2 be initialized by Make() to Follower
	// l3 be write by StartElection()
	// l4 be read by StartElection(), ticker()

	timeoutEventCh chan time.Time // l1 for start or restart leader election.
	// l2 producer: ticker(), consumer: StartElection()
	resetTimerEventCh chan time.Time // l1 for reset timer
	// l2 producer: AppendEntries() & RequestVote(), consumer: ticker()
	stopElectionEventCh chan time.Time // l1 for stop leader election
	// l2 producer: AppendEntries() & RequestVote(), consumer: StartElection()

	replicatorCond []*sync.Cond // for signal replicator to send LogEntry in batch
	heartbeatCond  *sync.Cond   // for signal heartbeat

	applyCh   chan ApplyMsg
	applyCond *sync.Cond // used to signal applier goroutine to batch replicating entries
}

func (rf *Raft) lock(user string) {
	rf.mu.Lock()
	TPrintf("peer %d func %v lock the Raft\n", rf.me, user)
}

func (rf *Raft) unlock(user string) {
	rf.mu.Unlock()
	TPrintf("peer %d func %v unlock the Raft\n", rf.me, user)
}

func (rf *Raft) getRelativeIndex(absoluteIndex int) int {
	return absoluteIndex - rf.log[0].Index
}

func (rf *Raft) getAbsoluteIndex(relativeIndex int) int {
	return relativeIndex + rf.log[0].Index
}

func ProduceEventToChannel(ch chan time.Time) {
	for {
		select {
		case ch <- time.Now():
			return
		case <-ch:
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock("GetState")
	defer rf.unlock("GetState")

	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	var currentTerm int
	var voteFor int
	var log []LogEntry
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("decode error\n")
		return
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.log = log
	rf.commitIndex = rf.getFirstLog().Index
	rf.lastApplied = rf.getFirstLog().Index
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.lock("CondInstallSnapshot")
	defer rf.unlock("CondInstallSnapshot")
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// 有两种方案, 一种是与 lastApplied 比较, 另一种是与 commitIndex 比较
	// 前者, 在确保状态机的状态不会回滚的情况下, 优先进行快照恢复; 后者, 在相同情况下, 使状态机优先执行本地命令
	//
	// 新发现, 如果在 applier 完成 applies [lastApplied+1, commitIndex] 与 update lastApplied 之间,
	// 发生了 CondInstallSnapshot, 且恰好 lastApplied < lastIncludedIndex < commitIndex,
	// 这会导致状态机回滚, 且 Raft applier 依旧将 lastApplied 更新成 commitIndex, 即 Raft 与 状态机出现不一致
	// 问题的核心在于 applier 的 applying 和 update lastApplied 不是原子的, 为了避免状态机回滚, 要保证 CondInstallSnapshot 的恢复位置
	// 不能在 lastApplied < lastIncludedIndex < commitIndex, 要在 lastIncludedIndex >= commitIndex
	// 结论, 前者会出现问题, 不可取
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger",
			rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex <= rf.getLastLog().Index && lastIncludedTerm == rf.getLog(lastIncludedIndex).Term {
		rf.shrinkLog(lastIncludedIndex)
	} else {
		rf.log = []LogEntry{{Command: nil, Index: lastIncludedIndex, Term: lastIncludedTerm}}
	}
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex

	DPrintf("{Node %v}'s state is {Term %v, commitIndex %v, lastApplied %v, firstLog %v, lastLog %v}"+
		" after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.currentTerm,
		rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

func (rf *Raft) shrinkLog(retainIdx int) {
	if retainIdx < rf.getFirstLog().Index || retainIdx > rf.getLastLog().Index {
		return
	}
	retainLen := rf.getLastLog().Index - retainIdx + 1
	shrinking := 4
	if retainLen*shrinking < cap(rf.log) {
		newLog := make([]LogEntry, retainLen)
		copy(newLog, rf.log[rf.getRelativeIndex(retainIdx):])
		rf.log = newLog
	} else {
		copy(rf.log, rf.log[rf.getRelativeIndex(retainIdx):])
		rf.log = rf.log[:retainLen]
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock("Snapshot")
	defer rf.unlock("Snapshot")

	firstIndex := rf.getFirstLog().Index
	if index <= firstIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger int term"+
			" %v", rf.me, index, rf.getFirstLog().Index, rf.currentTerm)
		return
	}

	// 有一种比较常见的场景, applier 批量地提交命令给状态机, 未及时更新 lastApplied, 这时状态机调用了 Snapshot, 那么此时 index > lastApplied,
	// 需要及时地更新 lastApplied, 以防止 CondInstallSnapshot 的误判
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.shrinkLog(index)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	// {Node 0}'s state is {Term 1, commitIndex 2592, lastApplied 2570, firstLog 2573, lastLog 2593
	// after replacing log with snapshotIndex 2573 as old snapshotIndex 2572 is smaller
	DPrintf("{Node %v}'s state is {Term %v, commitIndex %v, lastApplied %v, firstLog %v, lastLog %v} "+
		"after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.currentTerm,
		rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, firstIndex)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock("Start")
	defer rf.unlock("Start")

	if rf.role != Leader {
		return -1, -1, false
	}

	newEntry := rf.appendNewEntry(command)
	DPrintf("Peer %v receives a new command[%v] to replicateOneRound in term %v", rf.me, newEntry, rf.currentTerm)
	rf.BroadcastHeartbeat(false)

	return newEntry.Index, newEntry.Term, true
}

func (rf *Raft) appendNewEntry(command interface{}) LogEntry {
	newEntry := LogEntry{Command: command, Term: rf.currentTerm, Index: rf.getAbsoluteIndex(len(rf.log))}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	return newEntry
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("peer %d has been killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 生成随机时间段 (min, max)
func randomizedDuration(min, max int64) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(min+rand.Int63n(max-min)) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// todo: redesign ticker()
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		for {
			rf.lock("ticker")
			if rf.role != Leader {
				rf.unlock("ticker")
				break
			}
			rf.unlock("ticker")
			time.Sleep(30 * time.Millisecond)
		}

		select {
		case <-time.After(randomizedDuration(TimeoutMin, TimeoutMax)):
			rf.lock("ticker")
			if rf.role == Follower {
				DPrintf("peer %d election timeout\n", rf.me)
				go rf.StartElection()
			} else if rf.role == Candidate {
				ProduceEventToChannel(rf.timeoutEventCh)
			}
			rf.unlock("ticker")
		case <-rf.resetTimerEventCh:
		}
	}
}

func (rf *Raft) applier() {

	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		currentTerm := rf.currentTerm
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[rf.getRelativeIndex(lastApplied+1):rf.getRelativeIndex(commitIndex)+1])
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		DPrintf("{Peer %v} applies entries %v-%v %v in term %v", rf.me, lastApplied+1, commitIndex, entries, currentTerm)
		// 可能有CondInstallSnapshot将lastApplied更新为了介于oldLastApplied和commitIndex之间的值, 这使得状态机回滚
		rf.mu.Lock()
		// During sending ApplyMsg, the lastApplied may have been updated by the snapshot
		rf.lastApplied = max(commitIndex, rf.lastApplied)
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:               peers,
		persister:           persister,
		me:                  me,
		currentTerm:         0,
		voteFor:             -1,
		log:                 []LogEntry{{Command: nil, Term: 0, Index: 0}},
		commitIndex:         0,
		lastApplied:         0,
		nextIndex:           make([]int, len(peers)),
		matchIndex:          make([]int, len(peers)),
		role:                Follower,
		timeoutEventCh:      make(chan time.Time, 1),
		resetTimerEventCh:   make(chan time.Time, 1),
		stopElectionEventCh: make(chan time.Time, 1),
		replicatorCond:      make([]*sync.Cond, len(peers)),
		applyCh:             applyCh,
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartbeatCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for peer := 0; peer < len(peers); peer++ {
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = lastLog.Index + 1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(new(sync.Mutex))
			go rf.replicator(peer)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeatScheduler to send heartbeatPeriodically
	go rf.scheduleHeartbeat()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	DPrintf("{Node %d} starts, which state is {Term %v, voteFor %v, firstLog %v, lastLog %v, commitIndex %v, "+
		"lastApplied %v}", rf.me, rf.currentTerm, rf.voteFor, rf.getFirstLog(), rf.getLastLog(), rf.commitIndex,
		rf.lastApplied)

	return rf
}
