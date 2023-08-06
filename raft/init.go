package raft

import (
	"DDB/client"
)

type InitArgs struct {
	Client []*client.Client
}

type InitReply struct {
	Client []*client.Client
}

func (rf *Raft) containPeer(cl *client.Client) bool {
	for _, peer := range rf.peers {
		if string(peer.Ip) == string(cl.Ip) {
			return true
		}
	}
	return false
}

func (rf *Raft) Init(args *InitArgs, reply *InitReply) error {
	for _, peer := range args.Client {
		if !rf.containPeer(peer) {
			rf.peers = append(rf.peers, peer)
		}
	}
	reply.Client = rf.peers
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	return nil
}

func (rf *Raft) setInit() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := InitArgs{}
		reply := InitReply{}
		args.Client = rf.peers
		rf.peers[peer].Call("Raft.Init", args, reply)
		for _, peer := range reply.Client {
			if !rf.containPeer(peer) {
				rf.peers = append(rf.peers, peer)
			}
		}
	}
}
