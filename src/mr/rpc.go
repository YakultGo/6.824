package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// example to show how to declare the arguments
// and reply for an RPC.

const (
	mapTask    string = "map"
	reduceTask string = "reduce"
)

type Task struct {
	Type         string
	Index        int
	MapInputFile string
	WorkerId     string
	Deadline     time.Time
}
type ApplyTaskArgs struct {
	WorkerId      string
	LastTaskType  string
	LastTaskIndex int
}
type ApplyTaskReply struct {
	TaskType     string
	TaskIndex    int
	MapInputFile string
	MapNum       int
	ReduceNum    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", worker, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}
