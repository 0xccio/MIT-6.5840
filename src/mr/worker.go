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

	// Your worker implementation here.
	for {
		task := CallForTask()

		switch task.TaskType{
		case MapTask:
			fmt.Println("Start HandleMapTask")
			err := HandleMapTask(task, mapf)
			if err != nil {
				log.Println("Worker: Map Task failed")
			} else {
				// TODO: 汇报Map任务执行状态
			}
		case ReduceTask:
			// TODO
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// CallForTask()
}

func HandleMapTask(task *Task, mapf func(string, string) []KeyValue) error {
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
	fmt.Println(midFile[0])
	sort.Sort(ByKey(midFile))
	reduceNum := task.ReducerNum
	HashedKV := make([][]KeyValue, reduceNum)

	for _, kv := range midFile {
		HashedKV[ihash(kv.Key)%reduceNum] = append(HashedKV[ihash(kv.Key)%reduceNum], kv)
	}
	for i := range reduceNum {
		oname := "mr-mid-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		folderPath := "/mnt/c/Users/17772957183/Desktop/ceg/博客/output/"
		ofile, _ := os.Create(folderPath+oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallForTask() *Task {
	req := Request{}
	req.TaskType = MapTask

	resp := Task{}
	ok := call("Coordinator.GetTask", &req, &resp)
	if ok {
		fmt.Printf("worker successfully get task: %s\n", resp.FileName)
	} else {
		fmt.Printf("worker get task failed\n")
	}
	return &resp
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
