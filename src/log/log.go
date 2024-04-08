package log

import (
	"fmt"
	"sync"
	"time"
)

func Debugf(format string, a ...any) (n int, err error) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	fmt.Printf("[%s] ", time.Now().Local().Format("20060102 15:04:05.0000"))
	n, err = fmt.Printf(format, a...)
	fmt.Println("")
	return
}