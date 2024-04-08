package raft

import (
	"time"

	"6.5840/log"
)

func (rf *Raft)becomeFollower() {
	rf.state = Follower

	log.Debugf("%d become follower", rf.me)
}

func (rf *Raft)becomeCandidate(){
	rf.state = Candidate
	rf.electionTime = time.Now()
	rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)
	log.Debugf("%d become candidate, term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.electionTime = time.Now()
	log.Debugf("%d become leader, term %d", rf.me, rf.currentTerm)
}