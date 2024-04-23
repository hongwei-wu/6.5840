package raft

import (
	"fmt"
	"sync"
	"time"
)

func (rf *Raft)stateToString(state int)string {
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

func (rf *Raft)Debugf(format string, a ...any) (n int, err error) {
	return 0, nil
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	fmt.Printf("[%s] ", time.Now().Local().Format("20060102 15:04:05.0000"))
	fmt.Printf("[%d-%s-%-2d] ", rf.me, rf.stateToString(rf.state), rf.currentTerm)
	n, err = fmt.Printf(format, a...)
	fmt.Println("")
	return
}