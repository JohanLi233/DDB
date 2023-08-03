package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func randTime() time.Duration {
	randTime := 100 + rand.Int63()%100
	return time.Duration(randTime) * time.Millisecond
}

func (rf *Raft) resetElection() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randTime())
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.votedFor = -1
	if term > rf.currentTerm {
		rf.currentTerm = term
	}
	rf.persist()
	rf.heartBeatTimer.Stop()
	rf.resetElection()
}
