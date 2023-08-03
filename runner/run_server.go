package main

import (
	"time"

	"6.5840/client"
	"6.5840/kvraft"
	"6.5840/raft"
)

func main() {
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.109")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.7")
	clients = append(clients, cl2)
	persister := raft.MakePersister()
	me := 1
	kv := kvraft.StartKVServer(clients, me, persister, -1)
	for !kv.Killed() {
		time.Sleep(100 * time.Minute)
	}
}
