package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 150~300ms
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(151)) * time.Millisecond
}

func min(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func cmd2Str(cmd interface{}) string {
	str := fmt.Sprint(cmd)
	if len(str) > 3 {
		str = str[:3]
	}
	return str
}
