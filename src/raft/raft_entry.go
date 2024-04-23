package raft

func (rf *Raft) entryLastIndex() int {
	return len(rf.entries)
}

func (rf *Raft) entryAt(index int) *RaftEntry {
	if index > len(rf.entries) {
		return nil
	}
	return rf.entries[index - 1]
}

func (rf *Raft) entryFromIndex(index int) []*RaftEntry {
	if index > len(rf.entries) {
		return nil
	}
	return rf.entries[index - 1:]
}

func(rf *Raft)entryAppend(entries []*RaftEntry) {
	rf.entries = append(rf.entries, entries...)
}

func (rf *Raft)entryPopFromIndx(index int) {
	rf.entries = rf.entries[0:index-1]
}
