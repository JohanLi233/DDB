package kvraft

import (
	"sync"
	"time"
)

const maxWaitTime = 2000 * time.Millisecond

type Notifier struct {
	done              sync.Cond
	maxRegisteredOpId int
}

func (kv *KVServer) makeNotifier(op *Op) {
	kv.getNotifier(op, true)
	kv.makeAlarm(op)
}

func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if notifer, ok := kv.notifier[op.ClerkId]; ok {
		notifer.maxRegisteredOpId = max(notifer.maxRegisteredOpId, op.OpId)
		return notifer
	}

	if !forced {
		return nil
	}

	notifier := new(Notifier)
	notifier.done = *sync.NewCond(&kv.mu)
	notifier.maxRegisteredOpId = op.OpId
	kv.notifier[op.ClerkId] = notifier

	return notifier
}

func (kv *KVServer) wait(op *Op) {
	// warning: we could only use `notifier.done.Wait` but there's a risk of spurious wakeup or
	// wakeup by stale ops.
	for !kv.Killed() {
		if notifier := kv.getNotifier(op, false); notifier != nil {
			notifier.done.Wait()
		} else {
			break
		}
	}
}

func (kv *KVServer) notify(op *Op) {
	if notifer := kv.getNotifier(op, false); notifer != nil {
		// only the latest op can delete the notifier.
		if op.OpId == notifer.maxRegisteredOpId {
			delete(kv.notifier, op.ClerkId)
		}
		notifer.done.Broadcast()
	}
}
