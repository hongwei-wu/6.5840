package raft

import (
	"time"
)

func (rf *Raft) tickLeader() {
	rf.brodcastHeartbeat()

	if !rf.isLeaderLeaseValid() {
		rf.Debugf("step down")
		rf.becomeFollower()
		return
	}
	rf.triggerApply()
}

func (rf *Raft) brodcastHeartbeat() {

	votes := 0
	for i := range rf.peerStates {
		if rf.state != Leader {
			break
		}
		if i == rf.me {
			votes += 1
			continue
		}

		if !rf.peerReplicateEntries(i) {
			continue
		}
		votes += 1
	}

	if votes >= len(rf.peers)/2+1 {
		rf.electionTime = time.Now()
	} else {
		rf.Debugf("recv only %d heartbeat response", votes)
	}
}

func (rf *Raft) isLeaderLeaseValid() bool {
	return rf.electionTime.Add(rf.electionTimeout).After(time.Now())
}

func (rf *Raft) replicateEntries(i int, prevLogIndex int, prevLogTerm int, entries []*RaftEntry) (*AppendEntriesReply, bool) {
	args := &AppendEntriesArgs{Term: rf.currentTerm,
		LeaderCommit: rf.commitIndex,
		LeaderId: rf.me,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm}
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(i, args, &reply)
	if !ok {
		return nil, false
	}
	return &reply, true
}

func (rf *Raft)updateCommit(index int) {
	if index <= rf.commitIndex {
		return
	}

	votes :=0
	for i :=range rf.peerStates {
		if i == rf.me {
			votes +=1
		}
		if rf.peerStates[i].MatchIndex < index {
			continue
		}
		votes += 1
	}

	rf.Debugf("check commit index %d %d", index, votes)
	if votes >= len(rf.peers)/2+1 {
		rf.commitIndex = index
		rf.Debugf("update commit index %d", index)
	}
}

func (rf *Raft) peerReplicateEntries(i int) bool {
	var prevLogIndex int
	var prevLogTerm int
	p := rf.peerStates[i]

	if p.NextIndex == 1 {
		prevLogIndex = 0
		prevLogTerm = 0
	} else {
		entry := rf.entryAt(p.NextIndex - 1)
		prevLogIndex = entry.Index
		prevLogTerm = entry.Term
	}
	entries := rf.entryFromIndex(p.NextIndex)

	reply, ok := rf.replicateEntries(i, prevLogIndex, prevLogTerm, entries)
	if !ok {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.Debugf("aer bigger term %d", reply.Term)
		rf.becomeFollower()
		rf.currentTerm = reply.Term
		return false
	}

	if !reply.Success {
		p.NextIndex = p.MatchIndex + 1
		return true
	}

	p.MatchIndex = reply.LastLogIndex
	p.NextIndex = p.MatchIndex + 1

	rf.Debugf("%d match index %d", i, p.MatchIndex)
	rf.updateCommit(p.MatchIndex)

	return true
}

func (rf *Raft) broadCastEntries() {
	for i := range rf.peerStates {
		if i == rf.me {
			continue
		}
		rf.peerReplicateEntries(i)
		if rf.state != Leader {
			break
		}
	}
}
