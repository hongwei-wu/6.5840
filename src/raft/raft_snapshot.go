package raft

import (
	"time"
)

func (rf *Raft) doSnapshot(index int, snapshot []byte) {

	rf.Debugf("take snapshot at %d bytes %d", index, len(snapshot))
	entry := rf.entryAt(index)
	if entry == nil {
		rf.Errf("no entry at %d", index)
		return
	}

	rf.entryPullFromIndex(index - 1)
	rf.snapshotIndex = index
	rf.snapshotTerm = entry.Term
	rf.snapshot = snapshot
	rf.persist()

}

func (rf *Raft) installSnapshot(snapshotIndex int, snapshotTerm int, snapshot []byte) {
	defer createChecker(rf, "installSnapshot").close()

	if rf.snapshotIndex == snapshotIndex {
		return
	}
	rf.Debugf("install snapshot at index %d bytes %d", snapshotIndex, len(snapshot))
	msg := ApplyMsg{SnapshotValid: true, SnapshotIndex: snapshotIndex, SnapshotTerm: snapshotTerm,
		Snapshot: snapshot}
	rf.applyCh <- msg

	rf.commitIndex = snapshotIndex
	rf.lastApplied = snapshotIndex
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
	rf.snapshot = clone(snapshot)
	rf.entryPullFromIndex(snapshotIndex)
	rf.persist()
}

func (rf *Raft) handleInstallSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.Debugf("snapshot bigger term %d", args.Term)
		rf.becomeFollower()
		rf.updateTermAndVote(args.Term, -1)
	}
	rf.becomeFollower()
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	rf.electionTime = time.Now()
	rf.installSnapshot(args.SnapshotIndex, args.SnapshotTerm, args.Snapshot)
}
