package raft

import (
	"time"

	"6.5840/log"
)

func (rf *Raft) tickCandidate() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		rf.startElection()
		return
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm += 1
	log.Debugf("%d change term %d", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{Term: rf.currentTerm,
		CandidateId: rf.me, LastLogIndex: 0, LastLogIndexTerm: 0}
	var reply RequestVoteReply

	votes := 0
	for i := range rf.peers {
		if i == rf.me {
			votes += 1
			continue
		}
		ok := rf.sendRequestVote(i, args, &reply)
		if !ok {
			continue
		}

		if reply.VoteGranted == 0 {
			continue
		}

		log.Debugf("%d get vote from %d, term %d/%d", rf.me, i, rf.currentTerm, reply.Term)
		votes += 1
	}

	if votes >= len(rf.peers)/2+1 {
		rf.becomeLeader()
		rf.brodcastHeartbeat()
	}

}

func (rf *Raft) brodcastHeartbeat() {
	args := &AppendEntriesArgs{Term: rf.currentTerm}
	var reply AppendEntriesReply

	votes := 0
	for i := range rf.peers {
		if i == rf.me {
			votes += 1
			continue
		}

		ok := rf.sendAppendEntries(i, args, &reply)
		if !ok {
			continue
		}

		if !reply.Success {
			continue
		}

		votes += 1
	}

	if votes >= len(rf.peers)/2+1 {
		rf.electionTime = time.Now()
	}
}
