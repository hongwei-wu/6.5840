package raft

import (
	"time"
)

func (rf *Raft) tickCandidate() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		if !rf.startElection(true) {
			goto reset_election_timeout
		}
		rf.startElection(false)
	reset_election_timeout:
		rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)
		rf.electionTime = time.Now()
		return
	}
}

func (rf *Raft) startElection(preVote bool) bool {
	if !preVote {
		rf.currentTerm += 1
		rf.Debugf("change term %d", rf.currentTerm)
	}

	var entry *RaftEntry
	args := &RequestVoteArgs{Term: rf.currentTerm,
		CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0,
		PreVote: preVote}

	if preVote {
		args.Term = rf.currentTerm + 1
	}

	if rf.entryLastIndex() != 0 {
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

		rf.Debugf("send %d rv term %d candidate id %d last index %d last term %d",
			i, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(i, args, &reply)
		if !ok {
			continue
		}

		if reply.VoteGranted == 0 {
			if reply.Term > rf.currentTerm {
				rf.Debugf("rvr bigger term %d", reply.Term)
				rf.currentTerm = reply.Term
				rf.becomeFollower()
				break
			}
			continue
		}

		rf.Debugf("get vote from %d, term %d/%d", i, rf.currentTerm, reply.Term)
		votes += 1
	}

	win := votes >= len(rf.peers)/2+1

	if !preVote {
		if win {
			rf.becomeLeader()
		}
	}
	return win
}
