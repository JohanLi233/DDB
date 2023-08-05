package raft

import "fmt"

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.resetElection()
	fmt.Println("reset peer")
	if rf.state == Candidate {
		rf.state = Follower
	}

	if rf.log.lastEntry().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log.Entries)
		return nil
	}

	if args.PrevLogIndex > rf.log.FirstIndex &&
		rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex - 1; xIndex > 0; xIndex-- {
			if xIndex-rf.log.FirstIndex < 0 {
				reply.XIndex = 0
				break
			}
			if rf.log.at(xIndex).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = len(rf.log.Entries)
		return nil
	}

	for idx, entry := range args.Entries {
		if entry.Index <= rf.log.lastEntry().Index && entry.Index > rf.log.FirstIndex &&
			rf.log.at(entry.Index).Term != entry.Term {
			rf.log.Entries = rf.log.sliceFromStart(entry.Index)
			rf.persist()
		}
		if entry.Index > rf.log.lastEntry().Index {
			rf.log.appendLog(args.Entries[idx:]...)
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastEntry().Index)
		rf.apply()
	}
	reply.Success = true
	return nil
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	fmt.Println("append")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}
	if args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		}
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[server] = max(rf.nextIndex[server], next)
			rf.matchIndex[server] = max(rf.matchIndex[server], match)
		} else if reply.Conflict {
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen
			} else {
				lastLogInXTerm := rf.log.findLastLogInTerm(reply.XTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[server] = lastLogInXTerm
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}
		} else if !reply.Success && rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
	}
}

func (rf *Raft) leaderAppendEntries() {
	rf.resetElection()
	fmt.Println("reset leader")
	lastLog := rf.log.lastEntry()
	for peer := range rf.peers {
		if rf.me == peer {
			rf.resetElection()
			fmt.Println("reset leader")
			continue
		}
		nextIndex := rf.nextIndex[peer]
		if lastLog.Index+1 < nextIndex {
			nextIndex = lastLog.Index
		}
		if nextIndex-1 < rf.log.FirstIndex {
			go rf.sendInstallSnapshot(peer)
			continue
		}
		prevLog := rf.log.at(nextIndex - 1)
		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogTerm = prevLog.Term
		args.PrevLogIndex = nextIndex - 1
		args.Entries = rf.log.sliceToEnd(nextIndex)
		go rf.sendAppendEntries(peer, &args, &reply)
	}
	rf.checkLeaderCommit()
}

func (rf *Raft) checkLeaderCommit() {
	if rf.state != Leader {
		return
	}
	count := make(map[int]int)
	for peer := range rf.peers {
		N := rf.matchIndex[peer]
		if N > rf.commitIndex {
			count[N] += 1
		}
	}
	for key, value := range count {
		if (value+1)*2 > len(rf.peers) && key > rf.commitIndex {
			rf.commitIndex = key
			rf.apply()
		}
	}
}
