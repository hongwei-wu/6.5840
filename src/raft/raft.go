package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

type RaftEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type PeerState struct {
	NextIndex          int
	MatchIndex         int
	RecentResponseTime time.Time
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	currentTerm            int // the current term
	votedFor               int // the peer that voted for this term
	entries                []*RaftEntry
	entriesCompactionIndex int

	taskCh chan func()
	// volatile state
	electionTime time.Time

	// Volatile state for all servers.
	commitIndex           int
	lastApplied           int
	state                 int
	electionTimeout       time.Duration
	randomElectionTimeout time.Duration
	heartbeatTimeout      time.Duration

	// Volatile state for leader.
	peerStates []*PeerState

	// Volatile state for follower.
	currentLeader int

	// Volatile state for candiate.

	// snapshot
	leaderSnapshotIndex int

	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term = int(rf.currentTerm)
	var isLeader = rf.state == Leader

	if rf.state == Leader {
		isLeader = rf.isLeaderLeaseValid()
	}
	// Your code here (3A).
	return term, isLeader
}

func (rf *Raft) encodeEntries(e *labgob.LabEncoder) {
	e.Encode(len(rf.entries))
	for i := range rf.entries {
		entry := rf.entries[i]
		e.Encode(entry.Term)
		e.Encode(entry.Index)
		e.Encode(entry.Command)
		//rf.Debugf("encode entry index %d term %d command %v", entry.Index, entry.Term, entry.Command)
	}
}

func (rf *Raft) decodeEntries(d *labgob.LabDecoder) {
	var size int

	d.Decode(&size)
	for i := 0; i < size; i++ {
		entry := &RaftEntry{}
		d.Decode(&entry.Term)
		d.Decode(&entry.Index)
		var command int
		d.Decode(&command)
		entry.Command = command

		//rf.Debugf("decode entry index %d term %d command %v", entry.Index, entry.Term, entry.Command)
		rf.entries = append(rf.entries, entry)
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	e.Encode(rf.entriesCompactionIndex)
	rf.encodeEntries(e)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	if err := d.Decode(&term); err != nil {
		rf.Errf("decode current term failed %s", err.Error())
		return
	}
	rf.currentTerm = term

	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		rf.Errf("decode voted for failed %s", err.Error())
		return
	}
	rf.votedFor = votedFor

	var index int
	if err := d.Decode(&index); err != nil {
		rf.Errf("decode snapshot index failed %s", err.Error())
		return
	}
	rf.snapshotIndex = index

	term = 0
	if err := d.Decode(&term); err != nil {
		rf.Errf("decode snapshot term failed %s", err.Error())
		return
	}
	rf.snapshotTerm = term

	index = 0
	if err := d.Decode(&index); err != nil {
		rf.Errf("decode entry compaction index failed %s", err.Error())
		return
	}
	rf.entriesCompactionIndex = index
	rf.decodeEntries(d)
}

// restore snapshot
func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var index int
	if err := d.Decode(&index); err != nil {
		rf.Errf("decode snapshot index failed %s", err.Error())
		return
	}
	rf.snapshotIndex = index

	var term int
	if err := d.Decode(&term); err != nil {
		rf.Errf("decode snapshot term failed %s", err.Error())
		return
	}
	rf.snapshotTerm = term

	var snapshot []byte
	if err := d.Decode(&snapshot); err != nil {
		rf.Errf("decode snapshot failed %s", err.Error())
		return
	}
	rf.snapshot = snapshot
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.doSnapshot(index, clone(snapshot))
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	PreVote      bool
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted int
}

type AppendEntriesArgs struct {
	Term          int
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []*RaftEntry
	LeaderCommit  int
	LeaderId      int
	SnapshotIndex int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	LastLogIndex int
}

type InstallSnapshot struct {
	Term          int
	SnapshotIndex int
	SnapshotTerm  int
	Snapshot      []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) checkPrevEntry(prevIndex int, prevTerm int) bool {
	if prevIndex == 0 {
		return true
	}

	if prevIndex == rf.snapshotIndex && prevTerm == rf.snapshotTerm {
		return true
	}

	entry := rf.entryAt(prevIndex)
	if entry == nil {
		return false
	}

	return entry.Term == prevTerm
}

func (rf *Raft) deleteConflictEntries(entries []*RaftEntry) (int, bool) {
	var i int

	for i = 0; i < len(entries); i++ {
		index := entries[i].Index
		entry := rf.entryAt(index)
		if entry == nil {
			return i, true
		}
		if entry.Term != entries[i].Term {
			rf.Debugf("entry term diff, index %d term %d/%d",
				entry.Index, entry.Term, entries[i].Term)
			rf.entryPopFromIndx(entry.Index)
			return i, true
		}
	}
	return i, true
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false

	rf.Debugf("recv %d ae term %d prev index %d prev term %d entries %d leader %d commit %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderId, args.LeaderCommit)
	if args.Term < rf.currentTerm {
		return
	}

	if rf.state != Follower {
		rf.becomeFollower()
	}
	if args.Term > rf.currentTerm {
		rf.updateTermAndVote(args.Term, 0)
	}
	rf.electionTime = time.Now()
	rf.leaderSnapshotIndex = args.SnapshotIndex

	rf.Debugf("recv append entries %d, local entries %d",
		len(args.Entries), rf.entryLastIndex())

	if !rf.checkPrevEntry(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		reply.LastLogIndex = rf.entryLastIndex()
		rf.Debugf("check prev entry failed %d", args.PrevLogIndex)
		return
	}

	i, ok := rf.deleteConflictEntries(args.Entries)
	if !ok {
		reply.Success = false
		reply.LastLogIndex = rf.entryLastIndex()
		rf.Debugf("delete conflict entry failed %d", args.PrevLogIndex)
		return
	}

	rf.Debugf("append entries %d/%d/%d, commit %d/%d", i, len(args.Entries[i:]), len(args.Entries),
		rf.commitIndex, args.LeaderCommit)
	rf.entryAppend(args.Entries[i:])
	reply.Success = true
	reply.LastLogIndex = rf.entryLastIndex()

	if args.LeaderCommit > rf.commitIndex && args.LeaderCommit >= rf.entryLastIndex() {
		rf.commitIndex = args.LeaderCommit
		rf.Debugf("update commit index %d", rf.commitIndex)

		rf.triggerApply()
	}

	rf.currentLeader = args.LeaderId
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	doneC := make(chan interface{})

	rf.taskCh <- func() {
		defer close(doneC)
		rf.handleInstallSnapshot(args, reply)
	}
	<-doneC
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	doneC := make(chan interface{})

	rf.taskCh <- func() {
		defer close(doneC)
		rf.handleAppendEntries(args, reply)
	}
	<-doneC
}

func (rf *Raft) rpcCall(server int, name string, args interface{}, reply interface{}) bool {
	now := time.Now()
	defer func(pre time.Time) {
		if pre.Add(time.Millisecond * 35).Before(time.Now()) {
			rf.Errf("rpc timeout %s.", time.Now().Sub(pre).String())
		}
	}(now)

	ch := make(chan AsyncRpcRequest, 1)
	go func(ch chan AsyncRpcRequest) {
		defer close(ch)
		ok := rf.peers[server].Call(name, args, reply)
		ch <- AsyncRpcRequest{args, reply, ok}
	}(ch)

	select {
	case async := <-ch:
		return async.ok
	case <-time.After(time.Millisecond * 30):
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.rpcCall(server, "Raft.AppendEntries", args, reply)
}

func (rf *Raft) handelRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = 0
	var entry *RaftEntry

	rf.Debugf("recv %d rv term %d candidate id %d last index %d last term %d",
		args.CandidateId,
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	lastEntryIndex := rf.snapshotIndex
	lastEntryTerm := rf.snapshotTerm
	if rf.state == Leader || (rf.state == Follower && rf.currentLeader != -1) {
		rf.Debugf("still has leader, reject rv")
		return
	}
	if args.Term < rf.currentTerm {
		rf.Debugf("recv rv smaller term %d/%d", rf.currentTerm, args.Term)
		return
	}

	if args.PreVote {
		goto compare_log
	}

	if args.Term == rf.currentTerm && (rf.votedFor != 0 && rf.votedFor != args.CandidateId) {
		rf.Debugf("already vote %d at term %d", rf.votedFor, rf.currentTerm)
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		rf.Debugf("already vote %d, grant again", args.CandidateId)
		goto granted
	}
compare_log:

	if rf.entryNum() != 0 {
		entry = rf.entryAt(rf.entryLastIndex())
		if entry.Term > rf.currentTerm {
			panic("")
		}
		lastEntryIndex = rf.entryLastIndex()
		lastEntryTerm = entry.Term
	}

	if lastEntryTerm < args.LastLogTerm {
		rf.Debugf("last term smaller, grant")
		goto granted
	}

	if lastEntryTerm == args.LastLogTerm && lastEntryIndex <= args.LastLogIndex {
		rf.Debugf("last entry older index %d term %d, grant", lastEntryIndex, lastEntryTerm)
		goto granted
	}
	rf.Debugf("rv not grant, log index %d log term %d", lastEntryIndex, lastEntryTerm)
	return
granted:
	reply.VoteGranted = 1
	if !args.PreVote {
		rf.updateTermAndVote(args.Term, args.CandidateId)

		if rf.state != Follower {
			rf.becomeFollower()
		}
		rf.electionTime = time.Now()
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	doneC := make(chan interface{})

	rf.taskCh <- func() {
		defer close(doneC)
		rf.handelRequestVote(args, reply)
	}
	<-doneC
}

type AsyncRpcRequest struct {
	args  interface{}
	reply interface{}
	ok    bool
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.rpcCall(server, "Raft.RequestVote", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	_, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, false
	}

	// Your code here (3B).
	result := &struct {
		Index int
		Term  int
	}{}
	doneC := make(chan interface{})

	rf.taskCh <- func() {
		defer close(doneC)

		entry := &RaftEntry{Term: rf.currentTerm, Index: rf.entryLastIndex() + 1, Command: command}

		rf.Debugf("append entry at %d term %d command %v", entry.Index, entry.Term, command)
		rf.entryAppend([]*RaftEntry{entry})
		rf.broadCastEntries()
		result.Index = entry.Index
		result.Term = entry.Term
	}
	<-doneC

	return result.Index, result.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) tick() {
	switch rf.state {
	case Leader:
		rf.tickLeader()
	case Candidate:
		rf.tickCandidate()
	case Follower:
		rf.tickFollower()
	default:
		panic("unknown state")
	}
}

func (rf *Raft) ticker() {
	ms := 50 * (rf.me + 1)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		select {
		case task := <-rf.taskCh:
			task()
		case <-time.After(rf.heartbeatTimeout):
			rf.tick()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.currentTerm = 1
	rf.currentLeader = -1
	rf.votedFor = 0
	rf.entries = make([]*RaftEntry, 0)
	rf.entriesCompactionIndex = 0
	rf.taskCh = make(chan func())

	rf.electionTimeout = 600 * time.Millisecond
	rf.randomElectionTimeout = randomElectionTimeout(rf.electionTimeout)
	rf.heartbeatTimeout = rf.electionTimeout / 3
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTime = time.Now()
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.snapshot = persister.ReadSnapshot()
	if rf.snapshotIndex != 0 {
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
	}

	rf.Debugf("start term %d vote-for %d entries %d",
		rf.currentTerm, rf.votedFor, len(rf.entries))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func randomElectionTimeout(electionTimeout time.Duration) time.Duration {
	return electionTimeout + time.Duration(rand.Int63n(int64(electionTimeout)))
}

func (rf *Raft) triggerApply() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		entry := rf.entryAt(i)
		if entry == nil {
			rf.Errf("null entry at %d last %d", i, rf.entryLastIndex())
			panic("")
		}
		if entry.Index != rf.lastApplied+1 {
			rf.Errf("invalid entry index %d", entry.Index)
			panic("")
		}
		msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index}
		rf.applyCh <- msg
		rf.lastApplied += 1
		rf.Debugf("update applied %d", rf.lastApplied)
	}
}

func (rf *Raft) updateTermAndVote(term int, vote int) {
	rf.currentTerm = term
	rf.votedFor = vote
	rf.Debugf("change term %d vote for %d", rf.currentTerm, rf.votedFor)
	rf.persist()
}
