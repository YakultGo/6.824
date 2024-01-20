package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce         int      // reduce任务的数量
	inputFile       []string // 待处理的文件
	mapTasks        []MapReduceTask
	reduceTasks     []MapReduceTask
	mapDoneCount    int        // map任务完成的数量，当=len(inputFile)表示完成
	reduceDoneCount int        // reduce任务完成的数量，=nReduce表示完成
	mu              sync.Mutex // 全局锁
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// map阶段还没完成
	if c.mapDoneCount < len(c.inputFile) {
		// 遍历寻找还未完成的map任务
		for i := range c.mapTasks {
			// 如果该任务还没有分配，则将该任务分配给worker，并且将状态修改为已分配
			if c.mapTasks[i].Status == Unassigned {
				c.mapTasks[i].Status = Assigned
				c.mapTasks[i].StartTime = time.Now()
				reply.Task = c.mapTasks[i]
				return nil
			}
		}
		// 没找到未分配的，则等待已经分配的map任务完成
		reply.Task.TaskType = Wait
		return nil
	}
	// map阶段完成，则进行reduce阶段的任务
	if c.reduceDoneCount < c.nReduce {
		// 遍历寻找还未完成的reduce任务
		for i := range c.reduceTasks {
			// 如果该任务还没有分配，则将该任务分配给worker，并且将状态修改为已分配
			if c.reduceTasks[i].Status == Unassigned {
				c.reduceTasks[i].Status = Assigned
				c.reduceTasks[i].StartTime = time.Now()
				reply.Task = c.reduceTasks[i]
				return nil
			}
		}
		// 没找到未分配的，则等待已经分配的map任务完成
		reply.Task.TaskType = Wait
		return nil
	}
	// map和reduce都已经完成，则通知worker退出
	reply.Task.TaskType = Exit
	return nil
}

func (c *Coordinator) AcceptTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := args.Task.Id
	if args.Task.TaskType == Map {
		c.mapTasks[idx].Status = Finished
		c.mapDoneCount++
	} else if args.Task.TaskType == Reduce {
		c.reduceTasks[idx].Status = Finished
		c.reduceDoneCount++
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 所有reduce任务完成，则结束
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceDoneCount == c.nReduce {
		ret = true
		// 额外多做一个工作，就map任务中间产生的文件删掉，形如mr-X-Y
		for i := 0; i < len(c.inputFile); i++ {
			for j := 0; j < c.nReduce; j++ {
				os.Remove(tempFile(i, j))
			}
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化Coordinator
	c.nReduce = nReduce
	c.inputFile = files
	c.mapTasks = make([]MapReduceTask, len(files))
	c.reduceTasks = make([]MapReduceTask, nReduce)
	c.mapDoneCount = 0
	c.reduceDoneCount = 0
	c.mu = sync.Mutex{}
	// 初始化Map任务，每一个分配一个输入文件
	for i, file := range files {
		c.mapTasks[i] = MapReduceTask{
			TaskType:  Map,
			Status:    Unassigned,
			InputFile: []string{file},
			Id:        i,
			NReduce:   nReduce,
		}
	}
	// 初始化reduce任务，输入文件名已经是事先确定好的
	for i := 0; i < c.nReduce; i++ {
		var reduceInputFiles []string
		for j := 0; j < len(files); j++ {
			reduceInputFiles = append(reduceInputFiles, tempFile(j, i))
		}
		c.reduceTasks[i] = MapReduceTask{
			TaskType:  Reduce,
			Status:    Unassigned,
			InputFile: reduceInputFiles,
			Id:        i,
			NReduce:   nReduce,
		}
	}
	c.server()
	// 启动超时检查协程
	go c.checkTimeOut()
	return &c
}

func (c *Coordinator) checkTimeOut() {
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		// 首先判断map任务中超时的
		if c.mapDoneCount < len(c.inputFile) {
			// 遍历map任务中状态为Assigned的
			for i := 0; i < len(c.inputFile); i++ {
				if c.mapTasks[i].Status == Assigned {
					now := time.Now()
					// 已经启动超过10s，则放回未完成的任务中去
					if now.Sub(c.mapTasks[i].StartTime) > 10*time.Second {
						c.mapTasks[i].Status = Unassigned
					}
				}
			}
		}
		// 其次判断reduce任务中超时的
		if c.reduceDoneCount < c.nReduce {
			// 遍历reduce任务中状态为Assigned的
			for i := 0; i < c.nReduce; i++ {
				if c.reduceTasks[i].Status == Assigned {
					now := time.Now()
					// 已经启动超过10s，则放回未完成的任务中去
					if now.Sub(c.reduceTasks[i].StartTime) > 10*time.Second {
						c.reduceTasks[i].Status = Unassigned
					}
				}
			}
		}
		c.mu.Unlock()
	}
}
