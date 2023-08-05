package raft

import (
	"fmt"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	lastLog := rf.log.lastEntry()
	upToDate := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElection()
		rf.persist()
	}

	return nil
}

func (rf *Raft) sendRequestVote(
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply,
	votes *int,
) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}
	if reply.Term > args.Term {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Term < args.Term {
		return
	}

	if reply.VoteGranted {
		*votes += 1
	}
	if *votes*2 > len(rf.peers) && rf.state == Candidate && rf.currentTerm == args.Term {
		lastLogIndex := rf.log.lastEntry().Index + 1
		for peer := range rf.peers {
			rf.matchIndex[peer] = 0
			rf.nextIndex[peer] = lastLogIndex
		}
		rf.state = Leader
		rf.heartBeatTimer.Stop()
		fmt.Println("I am the leader")
		rf.heartBeatTimer.Reset(10 * time.Millisecond)
		rf.leaderAppendEntries()
	}
}

func (rf *Raft) candidateRequestVote(votes *int) {
	for peer := range rf.peers {
		if rf.me == peer {
			continue
		}
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		lastLog := rf.log.lastEntry()
		args.LastLogTerm = lastLog.Term
		args.LastLogIndex = lastLog.Index
		go rf.sendRequestVote(peer, &args, &reply, votes)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	votes := 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	rf.resetElection()
	rf.candidateRequestVote(&votes)
}
