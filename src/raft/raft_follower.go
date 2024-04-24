package raft

import "time"

func (rf *Raft) tickFollower() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		rf.becomeCandidate()
		return
	}
	rf.triggerApply()
}