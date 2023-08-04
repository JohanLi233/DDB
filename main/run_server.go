package main

import (
	"fmt"
	"time"

	"DDB/client"
	"DDB/kvraft"
	"DDB/raft"
)

func main() {
	clients := []*client.Client{}
	cl1 := client.MakeClient("192.168.0.109")
	clients = append(clients, cl1)
	cl2 := client.MakeClient("192.168.0.7")
	clients = append(clients, cl2)
	persister := raft.MakePersister()
	me := 0
	kv := kvraft.StartKVServer(clients, me, persister, 1000)
	fmt.Println("ok")
	for !kv.Killed() {
		time.Sleep(100 * time.Minute)
	}
}