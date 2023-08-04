package kvraft

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if kv.Killed() {
			return
		}
		kv.mu.Lock()
		if m.SnapshotValid {
			kv.ingestSnapshot(m.Snapshot)

		} else {
			op := m.Command.(*Op)
			if op.Type == "NoOp" {
				// skip no-ops.

			} else {
				kv.apply(op)
			}

			if kv.gc && kv.approachGCLimit() {
				kv.checkpoint(m.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) isApplied(op *Op) bool {
	max, ok := kv.maxApplied[op.ClerkId]
	return ok && max >= op.OpId
}

func (kv *KVServer) apply(op *Op) {
	if kv.isApplied(op) {
		return
	}
	switch op.Type {
	case "Get":

	case "Put":
		kv.db.Set(op.Key, op.Value)

	case "Append":
		previous, _ := kv.db.Get(op.Key)
		kv.db.Set(op.Key, op.Value+previous)
	}
	kv.maxApplied[op.ClerkId] = op.OpId
}

func (kv *KVServer) waitApply(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isApplied(op) {
		if !kv.start(op) {
			return ErrWrongLeader, ""
		}

		// wait until applied or timeout.
		kv.makeNotifier(op)
		kv.wait(op)
	}

	if kv.isApplied(op) {
		value := ""
		if op.Type == "Get" {
			// note: the default value, i.e. an empty string, is returned if the key does not exist.
			value, _ = kv.db.Get(op.Key)
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
