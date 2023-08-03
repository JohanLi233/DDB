package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files                  map[string]bool
	Reduces                map[int]bool
	Timers                 map[string]*time.Timer
	ReduceTimers           map[int]*time.Timer
	UnstartedReduces       map[int]bool
	MapTask                map[string]int
	UnstartedFiles         []string
	TempFiles              []string
	StopCounter            int
	NumberOfFiles          int
	NumberOfFilesCompleted int
	NReduce                int
	MapCompleted           bool
	ReduceCompleted        bool
	HaveUnstarted          bool
	HaveUnstartedReduce    bool
	mu                     sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Coordinator) AssignMapTask(args *AskArgs, reply *AskReplyArgs) error {
	m.mu.Lock()
	if len(m.UnstartedFiles) == 0 {
		reply.HaveUnstarted = false
		m.HaveUnstarted = false
	}
	if m.MapCompleted {
		reply.Completed = true
	}
	if m.HaveUnstarted {
		reply.FileName = m.UnstartedFiles[0]
		reply.HaveUnstarted = true
		reply.MapTask = m.MapTask[reply.FileName]
		reply.NReduce = m.NReduce
		m.UnstartedFiles = m.UnstartedFiles[1:]
		timer := time.NewTimer(time.Second * 10)
		m.Timers[reply.FileName] = timer
		go m.MapTimeout(reply.FileName, timer)
	}
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) MapTimeout(file string, timer *time.Timer) {
	<-timer.C
	m.mu.Lock()
	m.HaveUnstarted = true
	m.UnstartedFiles = append(m.UnstartedFiles, file)
	m.mu.Unlock()
}

func (m *Coordinator) ReduceTimeout(reduceTask int, timer *time.Timer) {
	<-timer.C
	m.mu.Lock()
	m.HaveUnstartedReduce = true
	m.UnstartedReduces[reduceTask] = true
	m.Reduces[reduceTask] = false
	m.mu.Unlock()
}

func (m *Coordinator) AskMapComplete(
	args *AskMapCompleteArgs,
	reply *AskMapCompleteReplyArgs,
) error {
	m.mu.Lock()
	reply.Completed = m.Files[args.FileName]
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) AskReduceComplete(
	args *AskReduceCompleteArgs,
	reply *AskReduceCompleteReplyArgs,
) error {
	m.mu.Lock()
	reply.Completed = m.ReduceCompleted
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) MarkMapTaskComplete(args *MarkArgs, reply *MarkReplyArgs) error {
	m.mu.Lock()
	m.TempFiles = append(m.TempFiles, args.TempFiles...)
	m.Files[args.FileName] = true
	m.Timers[args.FileName].Stop()
	m.NumberOfFilesCompleted += 1
	m.MapCompleted = true
	for _, value := range m.Files {
		if !value {
			m.MapCompleted = false
		}
	}
	reply.Completed = m.MapCompleted
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) MarkReduceTaskComplete(
	args *MarkReduceArgs,
	reply *MarkReduceReplyArgs,
) error {
	m.mu.Lock()
	m.Reduces[args.ReduceTask] = true
	m.ReduceTimers[args.ReduceTask].Stop()
	m.ReduceCompleted = true
	for _, value := range m.Reduces {
		if !value {
			m.ReduceCompleted = false
		}
	}

	reply.Completed = m.ReduceCompleted
	m.mu.Unlock()
	return nil
}

func (m *Coordinator) AssignReduceTask(args *AskReduceArgs, reply *AskReduceReplyArgs) error {
	m.mu.Lock()
	reply.ReduceTask = -1
	for key, value := range m.UnstartedReduces {
		m.HaveUnstartedReduce = false
		if value {
			reply.ReduceTask = key
			m.UnstartedReduces[key] = false
			m.HaveUnstartedReduce = true
			break
		}
	}
	if reply.ReduceTask == -1 {
		m.mu.Unlock()
		return nil
	}
	timer := time.NewTimer(time.Second * 10)
	m.ReduceTimers[reply.ReduceTask] = timer
	go m.ReduceTimeout(reply.ReduceTask, timer)
	m.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Coordinator) server() {
	if rpc.Register(m) != nil {
		log.Fatal("register error")
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Coordinator) clean() {
	for _, file := range m.TempFiles {
		os.Remove(file)
	}
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	if m.MapCompleted && m.ReduceCompleted {
		m.StopCounter++
		if m.StopCounter >= 2 {
			ret = true
			// remove temp files
			defer m.clean()
		}
	}
	m.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{}
	// Your code here.
	m.Files = make(map[string]bool)
	m.Timers = make(map[string]*time.Timer)
	m.ReduceTimers = make(map[int]*time.Timer)
	m.Reduces = make(map[int]bool)
	m.UnstartedReduces = make(map[int]bool)
	m.MapTask = make(map[string]int)

	for taskID, file := range files {
		m.Files[file] = false
		m.MapTask[file] = taskID
	}

	for i := 0; i < nReduce; i++ {
		m.Reduces[i] = false
		m.UnstartedReduces[i] = true
	}

	m.TempFiles = []string{}
	m.HaveUnstarted = true
	m.HaveUnstartedReduce = true
	m.NReduce = nReduce
	m.UnstartedFiles = files
	m.NumberOfFiles = len(files)
	m.StopCounter = 0

	m.server()
	return &m
}
