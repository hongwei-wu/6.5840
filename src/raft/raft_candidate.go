package raft

import (
	"time"
)

func (rf *Raft) tickCandidate() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		rf.startElection()
		rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)
		rf.electionTime = time.Now()
		return
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.Debugf("change term %d", rf.currentTerm)

	var entry *RaftEntry

	args := &RequestVoteArgs{Term: rf.currentTerm,
		CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0}


	if rf.entryLastIndex()  != 0 {
		entry = rf.entryAt(rf.entryLastIndex())
		args.LastLogIndex = entry.Index
		args.LastLogTerm = entry.Term
	}

	votes := 0
	for i := range rf.peers {
		if i == rf.me {
			votes += 1
			continue
		}
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(i, args, &reply)
		if !ok {
			continue
		}

		if reply.VoteGranted == 0 {
			if reply.Term > rf.currentTerm {
				rf.becomeFollower()
				break
			}
			continue
		}

		rf.Debugf("get vote from %d, term %d/%d", i, rf.currentTerm, reply.Term)
		votes += 1
	}

	if votes >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	}
}

