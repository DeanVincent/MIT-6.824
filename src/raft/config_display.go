package raft

func (cfg *config) displayLogEntries() {
	DPrintf("Logs:\n")
	cfg.mu.Lock()
	for i := 0; i < len(cfg.rafts); i++ {
		rf := cfg.rafts[i]
		if rf == nil {
			DPrintf("{Node %d}'s state is nil\n", i)
			continue
		}
		rf.mu.Lock()
		if rf.role == Leader {
			DPrintf("{Node %d}'s state is {Role %8s, Term %v, commitIndex %v, lastApplied %v, firstLog %v, "+
				"lastLog %v, matchIndex %v nextIndex %v}\n",
				rf.me, rf.getRoleName(rf.role), rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(),
				rf.getLastLog(), rf.matchIndex, rf.nextIndex)
		} else {
			DPrintf("{Node %d}'s state is {Role %8s, Term %v, commitIndex %v, lastApplied %v, firstLog %v, "+
				"lastLog %v}\n",
				rf.me, rf.getRoleName(rf.role), rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(),
				rf.getLastLog())
		}
		rf.mu.Unlock()
	}
	cfg.mu.Unlock()
}
