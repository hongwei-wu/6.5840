package raft

import (
	"time"
)

func (rf *Raft) tickLeader() {
	rf.brodcastHeartbeat()

	if rf.electionTime.Add(rf.electionTimeout - rf.heartbeatTimeout).Before(time.Now()) {
		rf.updateLeaderLease()
	}

	if !rf.isLeaderLeaseValid() {
		rf.Debugf("step down")
		rf.becomeFollower()
		return
	}
	//lastApplied := rf.lastApplied
	rf.triggerApply()
	//rf.Errf("tick leader last %d commit %d applied %d/%d, min match %d", rf.entryLastIndex(),
	//	rf.commitIndex, lastApplied, rf.lastApplied, rf.getMinMatchIndex())

}

func (rf *Raft) updateLeaderLease() {
	votes := 0
	for i, p := range rf.peerStates {
		if i == rf.me {
			votes += 1
			continue
		}
		if p.RecentRecv {
			votes += 1
			p.RecentRecv = false
		}
	}
	if votes >= len(rf.peers)/2+1 {
		rf.electionTime = time.Now()
	} else {
		rf.Debugf("recv only %d heartbeat response", votes)
	}
}

func (rf *Raft) brodcastHeartbeat() {
	for i, p := range rf.peerStates {
		if rf.state != Leader {
			break
		}
		if i == rf.me {
			continue
		}

		if p.RecentSentTime.Add(rf.heartbeatTimeout).After(time.Now()) {
			continue
		}

		p.RecentSentTime = time.Now()
		rf.peerReplicateEntries(i)
	}
}

func (rf *Raft) isLeaderLeaseValid() bool {
	return rf.electionTime.Add(rf.electionTimeout).After(time.Now())
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshot, reply *InstallSnapshotReply) bool {
	return rf.rpcCall(server, "Raft.InstallSnapshot", args, reply, rf.rpcTimeout)
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

func (rf *Raft) handleInstallSnapshotReply(i int, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.Debugf("aer bigger term %d", reply.Term)
		rf.becomeFollower()
		rf.updateTermAndVote(reply.Term, -1)
		return
	}

	if rf.state != Leader {
		return
	}

	rf.peerStates[i].MatchIndex = rf.snapshotIndex
	rf.peerStates[i].NextIndex = rf.snapshotIndex + 1
	rf.peerStates[i].RecentResponseTime = time.Now()
	rf.peerStates[i].Pipeline = true
	rf.Debugf("update peer %d match index %d next index %d", i,
		rf.snapshotIndex,
		rf.snapshotIndex+1)
	rf.peerReplicateEntries(i)
}

func (rf *Raft) peerReplicateSnapshot(i int) bool {
	go func(i int) {
		reply, ok := rf.replicateSnapshot(i, rf.snapshotIndex, rf.snapshotTerm)
		if !ok {
			return
		}
		select {
		case <-rf.doneCh:
		case rf.taskCh <- func() {
			rf.handleInstallSnapshotReply(i, reply)
		}:
		}
	}(i)

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

func (rf *Raft) handleAppendEntriesReply(i int, reply *AppendEntriesReply, prevLogIndex int, prevLogTerm int) {

	if reply.Term > rf.currentTerm {
		rf.Debugf("aer bigger term %d", reply.Term)
		rf.becomeFollower()
		rf.updateTermAndVote(reply.Term, -1)
		return
	}
	if rf.state != Leader {
		return
	}
	p := rf.peerStates[i]
	p.RecentRecv = true
	p.RecentResponseTime = time.Now()
	p.Applied = reply.Applied

	if !reply.Success {
		rf.Debugf("peer %d aer failed, prev %d/%d last %d match %d pipeline %t",
			i, prevLogIndex, prevLogTerm, reply.LastLogIndex, p.MatchIndex, p.Pipeline)
		if p.Pipeline {
			p.MatchIndex = 0
			p.NextIndex = rf.entryLastIndex()
			p.RecentResponseTime = time.Now()
			p.Pipeline = false
			return
		}

		if prevLogIndex >= reply.LastLogIndex+1 {
			p.NextIndex = reply.LastLogIndex + 1
			rf.Debugf("peer %d reset next index %d", i, p.NextIndex)
			return
		}

		for ; prevLogIndex > 0; prevLogIndex-- {
			term := rf.entryGetTerm(prevLogIndex, rf.snapshotIndex, rf.snapshotTerm, rf.entries)
			if term == 0 {
				break
			}

			followerTerm := rf.entryGetTerm(prevLogIndex, reply.FollowerPreLogIndex, reply.FollowerPreLogTerm, reply.Entries)
			if term == followerTerm {
				break
			}
		}
		p.NextIndex = prevLogIndex + 1
		rf.Debugf("peer %d reset next index %d", i, p.NextIndex)
		return
	}

	if rf.lastApplied < rf.commitIndex {
		rf.triggerApply()
	}

	p.Pipeline = true
	if reply.LastLogIndex > rf.entryLastIndex() {
		reply.LastLogIndex = rf.entryLastIndex()
		rf.Debugf("peer %d change lastLogIndex %d", i, reply.LastLogIndex)
	}

	if p.MatchIndex >= reply.LastLogIndex {
		return
	}
	p.MatchIndex = reply.LastLogIndex
	p.NextIndex = p.MatchIndex + 1
	rf.Debugf("%d match index %d", i, p.MatchIndex)

	entry := rf.entryAt(reply.LastLogIndex)
	if entry == nil {
		rf.Errf("entries %d %d reply.last %d",
			rf.entriesCompactionIndex, rf.entryNum(), reply.LastLogIndex)
		return

	}
	if entry.Term != rf.currentTerm {
		return
	}
	commitIndex := rf.commitIndex
	rf.updateCommit(p.MatchIndex)
	if commitIndex != rf.commitIndex {
		rf.broadCastEntries()
		rf.triggerApply()
	}
}

func (rf *Raft) peerReplicateEntries(i int) bool {
	var prevLogIndex int
	var prevLogTerm int

	p := rf.peerStates[i]

	if p.NextIndex == 1 {
		prevLogIndex = 0
		prevLogTerm = 0
		if rf.snapshotIndex != 0 {
			return rf.peerReplicateSnapshot(i)
		}
	} else {
		if p.NextIndex-1 == rf.snapshotIndex {
			prevLogIndex = rf.snapshotIndex
			prevLogTerm = rf.snapshotTerm
		} else {
			entry := rf.entryAt(p.NextIndex - 1)
			if entry == nil {
				rf.Debugf("peer %d no entry at index %d last index %d", i, p.NextIndex-1, rf.entryLastIndex())
				return rf.peerReplicateSnapshot(i)
			}
			prevLogIndex = entry.Index
			prevLogTerm = entry.Term
		}
	}
	entries := rf.entryFromIndex(p.NextIndex)
	go func(i int) {
		reply, ok := rf.replicateEntries(i, prevLogIndex, prevLogTerm, entries, false)
		if !ok {
			return
		}

		select {
		case <-rf.doneCh:
		case rf.taskCh <- func() {
			rf.handleAppendEntriesReply(i, reply, prevLogIndex, prevLogTerm)
		}:
		}
	}(i)

	return true
}

func (rf *Raft) broadCastEntries() {
	for i, p := range rf.peerStates {
		if i == rf.me {
			continue
		}
		p.RecentSentTime = time.Now()
		rf.peerReplicateEntries(i)
		if rf.state != Leader {
			break
		}
	}
}
