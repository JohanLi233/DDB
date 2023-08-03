package kvraft

func (kv *KVServer) start(op *Op) bool {
	_, _, isLeader := kv.rf.Start(op)
	return isLeader
}
