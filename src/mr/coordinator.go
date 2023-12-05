package mr

import (
	"fmt"
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
	lock           sync.Mutex
	stage          string // 空表示已完成可以退出
	nMap           int
	nReduce        int
	tasks          map[string]Task
	availableTasks chan Task
}

func (c *Coordinator) ApplyForTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	// 记录Worker的上一个Task已经运行完成
	if args.LastTaskType != "" {
		c.lock.Lock()
		lastTaskId := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskId]; exists && task.WorkerId == args.WorkerId {
			// 将该 Worker 的临时产出文件标记为最终产出文件
			if args.LastTaskType == mapTask {
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename(tmpMapOutFile(args.WorkerId, args.LastTaskIndex, ri), finalMapOutFile(args.LastTaskIndex, ri))
					if err != nil {
						return err
					}
				}
			} else if args.LastTaskType == reduceTask {
				err := os.Rename(tmpReduceOutFile(args.WorkerId, args.LastTaskIndex), finalReduceOutFile(args.LastTaskIndex))
				if err != nil {
					return err
				}
			}
			delete(c.tasks, lastTaskId)
			// 当前阶段所有 Task 已完成，进入下一阶段
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}
	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	task.WorkerId = args.WorkerId
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	return nil
}

func (c *Coordinator) transit() {
	if c.stage == mapTask {
		c.stage = reduceTask
		// 生成 Reduce Task
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  reduceTask,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == reduceTask {
		close(c.availableTasks)
		c.stage = "" // 使用空字符串标记作业完成
	}
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
	c.lock.Lock()
	defer c.lock.Unlock()
	ret = c.stage == ""
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.stage = mapTask
	c.nMap = len(files)
	c.nReduce = nReduce
	c.tasks = make(map[string]Task)
	c.availableTasks = make(chan Task, max(c.nMap, c.nReduce))
	for i, file := range files {
		task := Task{
			Type:         mapTask,
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	c.server()
	// 启动Task 自动回收过程
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != "" && time.Now().After(task.Deadline) {
					// 任务超时，重新分配
					task.WorkerId = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func GenTaskID(taskType string, index int) string {
	return fmt.Sprintf("%s-%d", taskType, index)
}
