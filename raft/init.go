package raft

import (
	"6.5840/client"
)

type InitArgs struct {
	Client []*client.Client
}

type InitReply struct {
	Client []*client.Client
}

func (rf *Raft) containPeer(cl *client.Client) bool {
	for _, peer := range rf.peers {
		if peer.Ip == cl.Ip {
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
	return nil
}

func (rf *Raft) setInit() {
	for peer := range rf.peers {
		args := InitArgs{}
		reply := InitReply{}
		args.Client = rf.peers
		rf.peers[peer].Call("Raft.Init", args, reply)
	}
}