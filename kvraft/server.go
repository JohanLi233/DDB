package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/client"
	"6.5840/labgob"

	"6.5840/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	maxApplied   map[int64]int
	persister    *raft.Persister
	gc           bool

	// Your definitions here.
	db       map[string]string
	notifier map[int64]*Notifier
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := Op{}
	op.ClerkId = args.ClerkId
	op.OpId = args.OpId
	op.Key = args.Key
	op.Type = "Get"
	err, value := kv.waitApply(&op)
	reply.Value = value
	reply.Err = err
	return nil
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	op := Op{}
	op.ClerkId = args.ClerkId
	op.OpId = args.OpId
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	err, _ := kv.waitApply(&op)
	reply.Err = err
	return nil
}

func StartKVServer(
	servers []*client.Client,
	me int,
	persister *raft.Persister,
	maxraftstate int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.mu = sync.Mutex{}

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.gc = maxraftstate != -1
	kv.persister = persister

	if kv.gc && kv.persister.SnapshotSize() > 0 {
		kv.ingestSnapshot(kv.persister.ReadSnapshot())

	} else {
		kv.db = make(map[string]string)
		kv.maxApplied = make(map[int64]int)
	}

	// You may need initialization code here.
	kv.notifier = make(map[int64]*Notifier)

	go kv.applier()

	return kv
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
