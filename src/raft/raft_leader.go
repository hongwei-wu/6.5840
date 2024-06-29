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

		if !rf.peerReplicateEntries(i, true) {
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshot, reply *InstallSnapshotReply) bool {
	return rf.rpcCall(server, "Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) replicateSnapshot(i int, snapshotIndex int, snapshotTerm int) (*InstallSnapshotReply, bool) {
	args := &InstallSnapshot{
		Term:          rf.currentTerm,
		SnapshotIndex: snapshotIndex,
		SnapshotTerm:  snapshotTerm,
		Snapshot:      rf.snapshot,
	}

	rf.Debugf("send %d snapshot index %d term %d", i, snapshotIndex, snapshotTerm)

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(i, args, &reply)
	if !ok {
		return nil, false
	}

	rf.Debugf("recv %d snapshot reply term %d", i, reply.Term)
	return &reply, ok
}

func (rf *Raft) peerReplicateSnapshot(i int) bool {
	reply, ok := rf.replicateSnapshot(i, rf.snapshotIndex, rf.snapshotTerm)
	if !ok {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.Debugf("aer bigger term %d", reply.Term)
		rf.becomeFollower()
		rf.updateTermAndVote(reply.Term, 0)
		return false
	}

	rf.peerStates[i].MatchIndex = rf.snapshotIndex
	rf.peerStates[i].NextIndex = rf.snapshotIndex + 1
	rf.peerStates[i].RecentResponseTime = time.Now()
	rf.Debugf("updater peer %d match index %d next index %d", i,
		rf.snapshotIndex,
		rf.snapshotIndex+1)
	return true
}

func (rf *Raft) replicateEntries(i int, prevLogIndex int, prevLogTerm int, entries []*RaftEntry, retry bool) (*AppendEntriesReply, bool) {
	args := &AppendEntriesArgs{Term: rf.currentTerm,
		LeaderCommit:  rf.commitIndex,
		LeaderId:      rf.me,
		Entries:       entries,
		PrevLogIndex:  prevLogIndex,
		PrevLogTerm:   prevLogTerm,
		SnapshotIndex: rf.snapshotIndex,
	}

	for i := 0; i < len(entries); i++ {
		if entries[i].Index != prevLogIndex+i+1 {
			panic("invalid entry index")
		}
	}

	rf.Debugf("send %d ae term %d prev index %d prev term %d entries %d leader %d commit %d",
		i, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderId, args.LeaderCommit)
	repeat := 0
retry:
	reply := AppendEntriesReply{}
	repeat += 1
	ok := rf.sendAppendEntries(i, args, &reply)
	if !ok {
		if repeat < 3 && retry {
			goto retry
		}
		return nil, false
	}
	rf.Debugf("recv %d aer term %d last index %d success %t", i, reply.Term, reply.LastLogIndex, reply.Success)
	return &reply, true
}

func (rf *Raft) updateCommit(index int) bool {
	if index <= rf.commitIndex {
		return false
	}

	votes := 0
	for i := range rf.peerStates {
		if i == rf.me {
			votes += 1
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
		return true
	}
	return false
}

func (rf *Raft) peerReplicateEntries(i int, retry bool) bool {
	var prevLogIndex int
	var prevLogTerm int
	p := rf.peerStates[i]

	if p.NextIndex == 1 {
		prevLogIndex = 0
		prevLogTerm = 0
		entry := rf.entryAt(p.NextIndex)
		if entry == nil {
			return rf.peerReplicateSnapshot(i)
		}
	} else {
		if p.NextIndex-1 == rf.snapshotIndex {
			prevLogIndex = rf.snapshotIndex
			prevLogTerm = rf.snapshotTerm
		} else {
			entry := rf.entryAt(p.NextIndex - 1)
			if entry == nil {
				rf.Errf("peer %d no entry at index %d last index %d", i, p.NextIndex-1, rf.entryLastIndex())
				return rf.peerReplicateSnapshot(i)
			}
			prevLogIndex = entry.Index
			prevLogTerm = entry.Term
		}
	}
	entries := rf.entryFromIndex(p.NextIndex)

	reply, ok := rf.replicateEntries(i, prevLogIndex, prevLogTerm, entries, retry)
	if !ok {
		return false
	}

	p.RecentResponseTime = time.Now()
	if reply.Term > rf.currentTerm {
		rf.Debugf("aer bigger term %d", reply.Term)
		rf.becomeFollower()
		rf.updateTermAndVote(reply.Term, 0)
		return false
	}

	if !reply.Success {
		p.NextIndex = p.MatchIndex + 1
		return true
	}

	if p.MatchIndex != reply.LastLogIndex && reply.LastLogIndex <= rf.entryLastIndex() {
		entry := rf.entryAt(reply.LastLogIndex)
		if entry.Term != rf.currentTerm {
			return true
		}
		p.MatchIndex = reply.LastLogIndex
		p.NextIndex = p.MatchIndex + 1

		rf.Debugf("%d match index %d", i, p.MatchIndex)
		if rf.updateCommit(p.MatchIndex) {
			rf.triggerApply()
		}
	}

	return true
}

func (rf *Raft) broadCastEntries() {
	for i := range rf.peerStates {
		if i == rf.me {
			continue
		}
		rf.peerReplicateEntries(i, false)
		if rf.state != Leader {
			break
		}
	}
}
