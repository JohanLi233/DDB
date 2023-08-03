package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type AskArgs struct {
}

type AskReplyArgs struct {
	FileName      string
	HaveUnstarted bool
	Completed     bool
	MapTask       int
	NReduce       int
}

type AskReduceArgs struct {
}

type AskReduceReplyArgs struct {
	ReduceTask int
}

type AskMapCompleteArgs struct {
	FileName string
}
type AskMapCompleteReplyArgs struct {
	Completed bool
}
type AskReduceCompleteArgs struct {
	Task int
}
type AskReduceCompleteReplyArgs struct {
	Completed bool
}

type MarkArgs struct {
	FileName  string
	TempFiles []string
}

type MarkReplyArgs struct {
	Completed bool
}

type MarkReduceArgs struct {
	ReduceTask int
}

type MarkReduceReplyArgs struct {
	Completed bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
