package raft

import "time"

func (rf *Raft) tickFollower() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		rf.becomeCandidate()
		rf.startElection()
		return
	}
	rf.triggerApply()
}