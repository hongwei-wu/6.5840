package raft

import (
	"bytes"
	"fmt"
)

func (rf *Raft) entryFirstIndex() int {
	return rf.entriesCompactionIndex + 1
}

func (rf *Raft) entryLastIndex() int {
	return rf.entriesCompactionIndex + len(rf.entries)
}

func (rf *Raft) entryAt(index int) *RaftEntry {
	if index <= rf.entriesCompactionIndex || index > rf.entriesCompactionIndex+len(rf.entries) {
		return nil
	}
	return rf.entries[index-rf.entriesCompactionIndex-1]
}

func (rf *Raft) entryFromIndex(index int) []*RaftEntry {
	if index > rf.entryLastIndex() {
		return nil
	}
	return rf.entries[index-rf.entriesCompactionIndex-1:]
}

func (rf *Raft) entryAppend(entries []*RaftEntry) {
	rf.entries = append(rf.entries, entries...)
	rf.persist()
}

func (rf *Raft) entryPopFromIndx(index int) {
	if index == 0 {
		panic("")
	}
	rf.entries = rf.entries[0 : index-rf.entriesCompactionIndex-1]
	rf.persist()
}

func (rf *Raft) entryPullFromIndex(index int) {
	if index < rf.entriesCompactionIndex {
		panic("")
	}

	if index > rf.entriesCompactionIndex+len(rf.entries) {
		rf.entriesCompactionIndex = index
		rf.entries = nil
		return
	}

	rf.entries = rf.entries[index-rf.entriesCompactionIndex:]
	rf.entriesCompactionIndex = index

}

func (rf *Raft) entryNum() int {
	return len(rf.entries)
}

func (rf *Raft) entryDump() {
	buf := bytes.NewBuffer(nil)
	for i, e := range rf.entries {
		if rf.entriesCompactionIndex+i+1 != e.Index {
			panic(fmt.Sprintf("%d %d %d", rf.entriesCompactionIndex, i, e.Index))
		}
		buf.WriteString(fmt.Sprintf("%d/%d ", e.Index, e.Term))
	}

	rf.Debugf("entries %s", buf.String())
}

func (rf *Raft) entryGetTerm(index int, prevIndex int, prevTerm int, entries []*RaftEntry) int {
	if index == prevIndex {
		return prevTerm
	}

	for i, e := range entries {
		if index == prevIndex+i+1 {
			return e.Term
		}
	}
	return 0
}
