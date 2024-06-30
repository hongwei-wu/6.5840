package raft

import (
	"fmt"
	"sync"
	"time"
)

var mu sync.Mutex

var enableDebug bool = false

func (rf *Raft) stateToString(state int) string {
	switch state {
	case Leader:
		return "L"
	case Follower:
		return "F"
	case Candidate:
		return "C"
	}
	panic("invalid state")
}

func (rf *Raft) fmtString(format string, a ...any) string {
	str := fmt.Sprintf("[%s %6.3fs]", time.Now().Local().Format("20060102 15:04:05.0000"),
		time.Since(rf.startTime).Seconds())
	str += fmt.Sprintf("[%d-%s-%d-%d] ", rf.me, rf.stateToString(rf.state), rf.currentTerm,
		len(rf.taskCh))
	str += fmt.Sprintf(format, a...)
	return str
}

func (rf *Raft) Debugf(format string, a ...any) (n int, err error) {
	if !enableDebug {
		return
	}
	str := rf.fmtString(format, a...)
	mu.Lock()
	defer mu.Unlock()

	return fmt.Println(str)
}

func (rf *Raft) Errf(format string, a ...any) (n int, err error) {
	str := rf.fmtString(format, a...)
	mu.Lock()
	defer mu.Unlock()

	return fmt.Println(str)
}

func (rf *Raft) Fatalf(format string, a ...any) (n int, err error) {
	str := rf.fmtString(format, a...)
	panic(str)
}
