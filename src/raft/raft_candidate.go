package raft

import (
	"time"
)

func (rf *Raft) tickCandidate() {
	if rf.electionTime.Add(rf.randomElectionTimeout).Before(time.Now()) {
		rf.startElection(true)
		rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)
		rf.electionTime = time.Now()
		return
	}
}

func (rf *Raft) handelRequestVoteReply(i int, reply *RequestVoteReply) {
	if reply.Term > rf.currentTerm {
		rf.Debugf("rvr bigger term %d", reply.Term)
		rf.updateTermAndVote(reply.Term, -1)
		rf.becomeFollower()
		return
	}

	if rf.state != Candidate || rf.preVote != reply.PreVote {
		return
	}

	if reply.VoteGranted == 0 {
		rf.Debugf("peer %d not grant, preVote %t", i, rf.preVote)
		return
	}
	rf.votes += 1
	rf.Debugf("peer %d grant, term %d/%d, votes %d preVote %t", i, rf.currentTerm, reply.Term, rf.votes,
		rf.preVote)

	win := rf.votes >= len(rf.peers)/2+1
	if !win {
		return
	}

	if rf.preVote {
		rf.startElection(false)
	} else {
		rf.becomeLeader()
	}
}

func (rf *Raft) startElection(preVote bool){
	if !preVote {
		rf.updateTermAndVote(rf.currentTerm+1, rf.me)
	}
	rf.preVote = preVote
	rf.votes = 1

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

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.Debugf("send %d rv term %d candidate id %d last index %d last term %d",
			i, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, &reply)
			if !ok {
				return
			}
			select {
			case <-rf.doneCh:
			case rf.taskCh <- func() {
				rf.handelRequestVoteReply(i, &reply)
			}:
			}
		}(i)
	}
}
