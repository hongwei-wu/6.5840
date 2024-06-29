package raft

import (
	"time"
)

func (rf *Raft) tickCandidate() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		if !rf.startElection(true) {
			goto reset_election_timeout
		}
		rf.Debugf("pre-vote succeed")
		rf.startElection(false)
	reset_election_timeout:
		rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)
		rf.electionTime = time.Now()
		return
	}
}

func (rf *Raft) startElection(preVote bool) bool {
	if !preVote {
		rf.updateTermAndVote(rf.currentTerm+1, rf.me)
	}

	var entry *RaftEntry
	args := &RequestVoteArgs{Term: rf.currentTerm,
		CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0,
		PreVote: preVote}

	if preVote {
		args.Term = rf.currentTerm + 1
	}

	if rf.entryNum() != 0 {
		entry = rf.entryAt(rf.entryLastIndex())
		args.LastLogIndex = entry.Index
		args.LastLogTerm = entry.Term
	} else if rf.snapshotIndex != 0 {
		args.LastLogIndex = rf.snapshotIndex
		args.LastLogTerm = rf.snapshotTerm
	}

	votes := 0
	for i := range rf.peers {
		if i == rf.me {
			votes += 1
			continue
		}

		repeat := 0
		rf.Debugf("send %d rv term %d candidate id %d last index %d last term %d",
			i, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	retry:
		repeat += 1
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(i, args, &reply)
		if !ok {
			if repeat < 3 {
				goto retry
			}
			continue
		}

		if reply.VoteGranted == 0 {
			if reply.Term > rf.currentTerm {
				rf.Debugf("rvr bigger term %d", reply.Term)
				rf.updateTermAndVote(reply.Term, 0)
				rf.becomeFollower()
				break
			}
			continue
		}

		votes += 1
		rf.Debugf("get vote from %d, term %d/%d, votes %d", i, rf.currentTerm, reply.Term, votes)

		if votes >= len(rf.peers)/2+1 {
			break
		}
	}

	win := votes >= len(rf.peers)/2+1

	if !preVote {
		if win {
			rf.becomeLeader()
		}
	}
	return win
}
