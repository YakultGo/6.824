package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
func RequestTask() *RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	res := call("Coordinator.AssignTask", &args, &reply)
	if !res {
		return nil
	}
	return &reply
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// 请求Coordinator分配任务
		res := RequestTask()
		if res == nil {
			return
		}
		switch res.Task.TaskType {
		case Map:
			doMap(&res.Task, mapf)
		case Reduce:
			doReduce(&res.Task, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			os.Exit(0)
		}
	}
}

/*
完成Map工作，输入文件只有1个，输出文件有nReduce个，存放在outputFiles中
*/
func doMap(task *MapReduceTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.InputFile[0])
	if err != nil {
		log.Fatalf("Map task %d, open file %s failed", task.Id, task.InputFile[0])
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Map task %d, read file %s failed", task.Id, task.InputFile[0])
	}
	file.Close()
	kva := mapf(task.InputFile[0], string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		// 获取key对应的所要存放的索引
		index := ihash(kv.Key) % task.NReduce
		hashedKva[index] = append(hashedKva[index], kv)
	}
	// 分别存到不同的文件中
	for i := 0; i < task.NReduce; i++ {
		file, _ := os.Create(tempFile(task.Id, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(file, "%v\t%v\n", kv.Key, kv.Value)
		}
		file.Close()
	}
	SubmitTask(task)
}

/*
完成Reduce工作，输入有len(inputFiles)个，输出只有一个
*/
func doReduce(task *MapReduceTask, reducef func(string, []string) string) {
	var lines []string
	// 将所有文件中的内容读取到lines中
	for i := 0; i < len(task.InputFile); i++ {
		file, err := os.Open(task.InputFile[i])
		if err != nil {
			log.Fatalf("Reduce task %d, open file %s failed", task.Id, task.InputFile[i])
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("Reduce task %d, read file %s failed", task.Id, task.InputFile[i])
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		// 去掉前缀和后缀的空格
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   split[0],
			Value: split[1],
		})
	}
	sort.Sort(ByKey(kva))
	reduceFile, err := os.Create(outFile(task.Id))
	if err != nil {
		log.Fatalf("Reduce task %d, generate %s file failed", task.Id, outFile(task.Id))
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(reduceFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	reduceFile.Close()
	SubmitTask(task)
}

/*
告知Coordinator，该任务已经完成
*/
func SubmitTask(task *MapReduceTask) {
	args := SubmitTaskArgs{Task: *task}
	reply := SubmitTaskReply{}
	res := call("Coordinator.AcceptTask", &args, &reply)
	if !res {
		return
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// 中间文件的名称
func tempFile(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId+1, reduceId+1)
}

// 最终文件的名称
func outFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId+1)
}
