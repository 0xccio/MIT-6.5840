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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	MapTask    TaskType = iota // Map 阶段任务
	ReduceTask                 // Reduce 阶段任务
)

// Add your RPC definitions here.
type Request struct {
	TaskType TaskType
	TaskID   int
}

type Task struct {
	TaskType   TaskType // 枚举类型，是 Map 任务还是 Reduce 任务
	FileName  string // 任务需要处理的输入文件
	TaskID     int      // 当前任务的id
	ReducerNum int      // Reduce 总数，用于决定中间结果的划分数量（即 y 的范围）
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
