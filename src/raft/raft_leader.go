package raft

import (
	"time"

	"6.5840/log"
)

func (rf *Raft) tickLeader() {
	rf.brodcastHeartbeat()

	if !rf.isLeaderLeaseValid(){
		log.Debugf("%d step down", rf.me)
		rf.becomeFollower()
		return
	}
}

func (rf *Raft) isLeaderLeaseValid() bool {
	return rf.electionTime.Add(rf.electionTimeout).After(time.Now())
}
