package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"DDB/client"
)

type Clerk struct {
	servers []*client.Client
	// You will have to modify this struct.
	id     int64
	leader int
	opId   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*client.Client) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.leader = 0
	ck.opId = 0
	return ck
}

func (ck *Clerk) allocateOpId() int {
	opId := ck.opId
	ck.opId++
	return opId
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.OpId = ck.allocateOpId()
	args.ClerkId = ck.id
	for {
		for i := range ck.servers {
			serverId := (ck.leader + i) % len(ck.servers)
			reply := GetReply{}
			ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.leader = serverId
					return reply.Value
				}
			}
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.OpId = ck.allocateOpId()
	args.Op = op
	args.Value = value
	args.ClerkId = ck.id
	for {
		for i := range ck.servers {
			fmt.Println(args.Key)
			serverId := (ck.leader + i) % len(ck.servers)
			reply := PutAppendReply{}
			ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == OK {
					ck.leader = serverId
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	fmt.Println(len(value))
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
