package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mapCompleted := false
	reduceCompleted := false
	for !mapCompleted {
		filename, haveUnstarted, mapTask, mapAssignCompleted, nReduce := AskMapTask()
		if mapAssignCompleted {
			break
		}
		if haveUnstarted {
			intermediate := []KeyValue{}
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)

			buckets := make([][]KeyValue, nReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%nReduce] = append(
					buckets[ihash(kva.Key)%nReduce],
					kva,
				)
			}

			fileMapCompleted := AskMapCompleted()
			if fileMapCompleted {
				break
			}

			tempFiles := []string{}

			// write into intermediate files
			for i := range buckets {
				oname := "mr-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(i) + "-temp"
				ofile, _ := os.Create(oname)
				tempFiles = append(tempFiles, oname)
				enc := json.NewEncoder(ofile)
				for _, kva := range buckets[i] {
					err := enc.Encode(&kva)
					if err != nil {
						log.Fatalf("cannot write into %v", oname)
					}
				}
				err := os.Rename(ofile.Name(), oname)
				if err != nil {
					log.Fatal(err)
				}
				ofile.Close()
			}

			mapCompleted = MarkMapTaskComplete(filename, tempFiles)
		}
	}

	for !reduceCompleted {
		if AskReduceComplete() {
			break
		}
		task := AskReduceTask()
		if task == -1 {
			continue
		}
		files, err := ioutil.ReadDir("./")
		reduceFiles := []*os.File{}
		if err != nil {
			log.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file.Name(), "-"+strconv.Itoa(task)+"-temp") {
				file, err := os.Open(file.Name())
				if err != nil {
					log.Fatal(err)
				}
				reduceFiles = append(reduceFiles, file)
			}
		}

		intermediate := []KeyValue{}
		for _, file := range reduceFiles {
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(task)
		ofile, _ := os.Create(oname)
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
		for _, file := range reduceFiles {
			file.Close()
		}

		reduceCompleted = MarkReduceTaskComplete(task)
	}

}

func AskReduceComplete() bool {
	askArgs := AskReduceCompleteArgs{}
	reply := AskReduceCompleteReplyArgs{}
	call("Coordinator.AskReduceComplete", &askArgs, &reply)
	return reply.Completed
}

func AskMapTask() (string, bool, int, bool, int) {
	askArgs := AskArgs{}
	reply := AskReplyArgs{}

	call("Coordinator.AssignMapTask", &askArgs, &reply)

	return reply.FileName, reply.HaveUnstarted, reply.MapTask, reply.Completed, reply.NReduce
}

func AskMapCompleted() bool {
	args := AskMapCompleteArgs{}
	reply := AskMapCompleteReplyArgs{}
	call("Coordinator.AskMapComplete", &args, &reply)
	return reply.Completed
}

func MarkMapTaskComplete(name string, files []string) bool {
	markArgs := MarkArgs{}
	markReplyArgs := MarkReplyArgs{}

	markArgs.FileName = name
	markArgs.TempFiles = files

	call("Coordinator.MarkMapTaskComplete", &markArgs, &markReplyArgs)
	return markReplyArgs.Completed
}

func AskReduceTask() int {
	askArgs := AskReduceArgs{}
	reply := AskReduceReplyArgs{}

	call("Coordinator.AssignReduceTask", &askArgs, &reply)

	return reply.ReduceTask
}

func MarkReduceTaskComplete(reduceTask int) bool {
	markArgs := MarkReduceArgs{}
	markReplyArgs := MarkReduceReplyArgs{}

	markArgs.ReduceTask = reduceTask

	call("Coordinator.MarkReduceTaskComplete", &markArgs, &markReplyArgs)
	return markReplyArgs.Completed
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
