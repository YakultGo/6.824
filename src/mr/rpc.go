package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
type taskType int

const (
	Map taskType = iota
	Reduce
	Exit
	Wait
)

type statusType int

const (
	Unassigned statusType = iota
	Assigned
	Finished
)

type MapReduceTask struct {
	// 0: Map, 1: Reduce, 2: Exit, 3: Wait
	TaskType taskType
	// 0: Unassigned, 1: Assigned, 2: Finished
	Status    statusType
	InputFile []string
	Id        int // 标识第几个map或者reduce
	NReduce   int // reduce任务的数量
	StartTime time.Time
}
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	Task MapReduceTask
}

type SubmitTaskArgs struct {
	Task MapReduceTask
}

type SubmitTaskReply struct {
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
