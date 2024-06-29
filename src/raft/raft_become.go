package raft

import (
	"time"
)

func (rf *Raft)becomeFollower() {
	rf.state = Follower
	rf.electionTime = time.Now()
	rf.currentLeader = -1

	rf.Debugf("become follower")
}

func (rf *Raft)becomeCandidate(){
	rf.state = Candidate
	rf.electionTime = time.Now()
	rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)

	rf.Debugf("become candidate")
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.electionTime = time.Now()
	rf.peerStates = make([]*PeerState, len(rf.peers))
	for i, p := range rf.peerStates {
		rf.peerStates[i] = &PeerState{}
		p = rf.peerStates[i]
		if i == rf.me {
			continue
		}
		p.MatchIndex = 0
		p.NextIndex = rf.entryLastIndex() + 1
		p.RecentResponseTime = time.Now()
		p.Pipeline = false
	}
	rf.Debugf("become leader")
}