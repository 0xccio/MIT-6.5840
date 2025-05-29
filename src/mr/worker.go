package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	alive := true


	// Your worker implementation here.
	for alive {
		task := CallForTask()

		switch task.TaskType{
		case MapTask:
			// fmt.Println("Start HandleMapTask")
			HandleMapTask(task, mapf)
			// fmt.Printf("Map task-%d is done, reporting status\n", task.TaskID)
			SendTaskStatus(task)
		case ReduceTask:
			// TODO
			if task.TaskID >= 8 {
				// fmt.Println("Start HandleReduceTask")
				HandleReduceTask(task, reducef)
				SendTaskStatus(task)
			}
		case WaittingTask:
			// TODO
			// fmt.Println("GetTask is waiting")
			time.Sleep(time.Second)
		case NoTask:
			time.Sleep(time.Second)
			// fmt.Println("Worker terminated........")
			alive = false
		}
		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// CallForTask()
}

func HandleMapTask(task *Task, mapf func(string, string) []KeyValue) error {
	// fmt.Printf("start handle Map Task-%d\n", task.TaskID)
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	midFile := mapf(task.FileName, string(content))

	// fmt.Println(midFile[0])

	sort.Sort(ByKey(midFile))
	reduceNum := task.ReducerNum
	HashedKV := make([][]KeyValue, reduceNum)

	for _, kv := range midFile {
		HashedKV[ihash(kv.Key)%reduceNum] = append(HashedKV[ihash(kv.Key)%reduceNum], kv)
	}
	for i := range reduceNum {
		oname := "mr-mid-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		// folderPath := "/mnt/c/Users/17772957183/Desktop/ceg/博客/output/"
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
	return nil
}

func HandleReduceTask(task *Task, reducef func(string, []string) string) {
	reduceFileNum := task.TaskID
	intermediate := readFromLocalFile(task.FileNames)
	sort.Sort(ByKey(intermediate))

	// 直接创建最终输出文件
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Failed to create output file %s: %v", oname, err)
	}
	defer ofile.Close()

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func CallForTask() *Task {
	req := Request{}

	resp := Task{}
	call("Coordinator.GetTask", &req, &resp)
	/*
	if ok {
		fmt.Printf("worker successfully get task: %s\n", resp.FileName)
	} else {
		fmt.Printf("worker get task failed\n")
	}
	*/
	return &resp
}

func SendTaskStatus(task *Task) {
	req := Request{
		TaskType: task.TaskType,
		TaskID: task.TaskID,
		TaskStatus: Finished,
	}
	resp := Task{}
	call("Coordinator.ReportTaskStatus", &req, &resp)
	/*
	if ok {
		fmt.Printf("Worker successfully reported status of task %d as %s\n", task.TaskID, "Finished")
	} else {
		fmt.Printf("Worker failed to report status of task %d\n", task.TaskID)
	}
	*/
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
