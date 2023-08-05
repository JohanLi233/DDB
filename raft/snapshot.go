package raft

import "fmt"

type Snapshot struct {
	Term  int
	Index int
	Data  []byte
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term     int
	CaughtUp bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}
	if args.LastIncludedIndex <= rf.commitIndex {
		reply.CaughtUp = true
		return nil
	}
	rf.becomeFollower(args.Term)

	rf.log.compactedTo(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persist()
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	reply.CaughtUp = true
	rf.snapshot.Data = args.Data
	rf.snapshot.Index = args.LastIncludedIndex
	rf.snapshot.Term = args.LastIncludedTerm

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	fmt.Println(">>>>>>>>>>")
	rf.ch <- msg
	return nil
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{}
	reply := InstallSnapshotReply{}

	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.snapshot.Index
	args.LastIncludedTerm = rf.snapshot.Term
	args.Data = rf.snapshot.Data

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.CaughtUp {
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1

		rf.leaderAppendEntries()
	}

}
