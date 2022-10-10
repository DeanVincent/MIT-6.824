// leader election
// 1. if a follower receives no communication over a period of time(election timeout), it begins an election
// 2. to begin an election, a follower increments its current Term and transitions to candidate state
// 3. vote for itself and send RequestVote RPCs in parallel to each of other servers in the cluster
// 4. a candidate continues in this state until one of three things happened:
// (a) it wins the election
// (b) another server establishes itself as leader
// (c) a period of time goes by with no winner
// 4(a) a candidate wins an election if it receive votes from a majority of this server in the full cluster for the same Term
// 4(b) while waiting for votes, a candidate may receive an AppendEntries RPC from another claiming to be leader.
// if the leader's Term as large as the candidate's current Term, then the candidate return to follower state.
// if the Term smaller than the candidate's current Term, then the candidate rejects the RPC and continues in candidate state
// 4(c) a candidate neither wins nor loses the election. when this happens, each candidate will time out and start a new
// election by increment its Term and initalizing another round of RequestVote RPCs.

package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int // candidate’s Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // Term of candidate’s last log entry (§5.4)
}

type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) CallRequestVote(peer int, votesChannel chan bool) {
	rf.lock("CallRequestVote")
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	reply := &RequestVoteReply{}
	rf.unlock("CallRequestVote")

	if rf.sendRequestVote(peer, args, reply) {
		rf.handleRequestVoteReply(peer, args, reply, votesChannel)
	}
}

func (rf *Raft) handleRequestVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply, votesChannel chan bool) {
	if reply.VoteGranted {
		votesChannel <- true
		return
	}

	rf.lock("handleRequestVoteReply")
	defer rf.unlock("handleRequestVoteReply")

	if args.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		if rf.role == Candidate {
			ProduceEventToChannel(rf.stopElectionEventCh)
		} else {
			rf.ChangeRole(Follower)
		}
		DPrintf("peer %v found newer term %v from peer %v than itself %v, update term to %v\n",
			rf.me, reply.Term, server, rf.currentTerm, reply.Term)
		rf.currentTerm, rf.voteFor = reply.Term, -1
		rf.persist()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout timeoutInterval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("peer %d send RequestVoteRPC to peer %d, args %v\n", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("peer %d's RequestVoteRPC %v to peer %d is missing\n", rf.me, args, server)
	}
	return ok
}

// RequestVote
// sendRequestVote RPC handler.
// logic:
// 1. Reply false if Term < currentTerm (§5.1)
// 2. If votedFor is null or CandidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("peer %d refused to vote for %d, because the candidate's term was smaller than his own\n", rf.me,
			args.CandidateId)
		return
	} else if args.Term == rf.currentTerm && rf.voteFor > -1 && rf.voteFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("peer %d refused to vote for %d, because it had already voted for someone else\n", rf.me,
			args.CandidateId)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
		if rf.role == Candidate {
			ProduceEventToChannel(rf.stopElectionEventCh)
		} else if rf.role == Leader {
			rf.ChangeRole(Follower)
		}
	}

	lastLog := rf.getLastLog()
	if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("peer %d refused to vote for %d, because last log entry is inconsistent\n", rf.me,
			args.CandidateId)
		return
	}

	ProduceEventToChannel(rf.resetTimerEventCh)
	rf.voteFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	DPrintf("peer %d agrees to vote for peer %d for term %d\n", rf.me, args.CandidateId, args.Term)
}

func (rf *Raft) getLog(absoluteIndex int) LogEntry {
	relativeIndex := rf.getRelativeIndex(absoluteIndex)
	if relativeIndex < 0 {
		DPrintf("{Node %v} getLog error: relativeIndex < 0, absoluteIndex %v, relativeIndex %v\n",
			rf.me, relativeIndex, absoluteIndex)
		return LogEntry{Command: nil, Term: 0, Index: 0}
	}
	return rf.log[relativeIndex]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) StartElection() {
	rf.lock("StartElection")
	rf.ChangeRole(Candidate)

	for rf.role == Candidate {
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.persist()
		votes := make(chan bool, len(rf.peers))
		votes <- true // votes for itself
		DPrintf("peer %d start election for term %d\n", rf.me, rf.currentTerm)
		rf.unlock("StartElection")

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.CallRequestVote(i, votes)
		}

		rf.countVotes(votes)
		rf.lock("StartElection")
	}
	rf.unlock("StartElection")
}

func (rf *Raft) countVotes(votesChannel chan bool) {
	votesTotal := 0
	for {
		select {
		case <-rf.stopElectionEventCh:
			rf.lock("StartElection")
			rf.ChangeRole(Follower)
			rf.unlock("StartElection")
			return
		case <-rf.timeoutEventCh:
			rf.lock("StartElection")
			DPrintf("peer %d stop election for term %v because timeout\n", rf.me, rf.currentTerm)
			rf.unlock("StartElection")
			return
		case <-votesChannel:
			votesTotal++
			// If votes received from majority of servers: become leader
			if votesTotal > len(rf.peers)/2 {
				rf.lock("StartElection")
				select {
				case <-rf.stopElectionEventCh:
					rf.ChangeRole(Follower)
					rf.unlock("StartElection")
					return
				default:
				}
				rf.ChangeRole(Leader)
				rf.unlock("StartElection")
				DPrintf("peer %d win the election for term %d\n", rf.me, rf.currentTerm)
				return
			}
		}
	}
}

func (rf *Raft) getRoleName(role Role) string {
	roleName, ok := nameMap[role]
	if !ok {
		roleName = ""
	}
	return roleName
}

func (rf *Raft) ChangeRole(role Role) {
	if rf.role == role {
		return
	}

	DPrintf("{Node %d} changes role from %s to %s in term %d", rf.me, rf.getRoleName(rf.role), rf.getRoleName(role), rf.currentTerm)
	rf.role = role

	switch rf.role {
	case Follower:
		ProduceEventToChannel(rf.resetTimerEventCh)
	case Candidate:
	case Leader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = lastLog.Index + 1
			rf.matchIndex[i] = 0
		}
		rf.heartbeatCond.Signal()
		// stop election timer
		ProduceEventToChannel(rf.resetTimerEventCh)
	}
}
