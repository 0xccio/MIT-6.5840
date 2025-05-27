package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

// Task 状态
const (
	Waiting  TaskStatus = iota // 等待运行
	Running                    // 正在运行
	Finished                   // 完成
	Failed                     // 失败
)

func (ts TaskStatus) String() string {
	switch ts {
	case Waiting:
		return "Waiting"
	case Running:
		return "Running"
	case Finished:
		return "Finished"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type TaskMetaInfo struct {
	Task       *Task
	StartTime  time.Time
	TaskStatus TaskStatus
}

type TaskCollection struct {
	muMap   sync.Mutex
	MetaMap map[int]*TaskMetaInfo
}

func (tc *TaskCollection) AddTask(taskMeta *TaskMetaInfo) {
	key := taskMeta.Task.TaskID
	if _, exist := tc.MetaMap[key]; exist {
		fmt.Printf("Task with ID %d already exists in the collection\n", key)
	} else {
		tc.MetaMap[key] = taskMeta
	}
}

func (tc *TaskCollection) GetTaskMetaInfo(taskId int) (*TaskMetaInfo, bool) {
	res, err := tc.MetaMap[taskId]
	return res, err
}

func (tc *TaskCollection) StartTask(taskId int) error {
	taskInfo, ok := tc.GetTaskMetaInfo(taskId)
	if !ok {
		return fmt.Errorf("task with ID %d not found", taskId)
	}
	if taskInfo.TaskStatus != Waiting {
		return fmt.Errorf("cannot start task with ID %d: current status is %s, expected Waiting", taskId, taskInfo.TaskStatus)
	}
	taskInfo.TaskStatus = Running
	taskInfo.StartTime = time.Now()
	return nil
}

func (tc *TaskCollection) checkTaskDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range tc.MetaMap {
		if v.Task.TaskType == MapTask {
			if v.TaskStatus == Finished {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.TaskStatus == Finished {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("%d/%d map tasks are done, %d/%d reduce tasks are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	return (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0)
}

type Condition int

const (
	MapPhase Condition = iota // Map阶段
	ReducePhase               // Reduce阶段
	AllDone                   // 全部完成
)

type Coordinator struct {
	// Your definitions here.
	Condition    Condition
	MapTaskCh    chan *Task
	ReduceTaskCh chan *Task
	ReducerNum   int
	MapNum       int
	GlobalTaskID int
	MapTasks     TaskCollection
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(req *Request, resp *Task) error {

	if c.Condition == MapPhase {
		// Map任务没有全部完成，分配一个给worker
		if len(c.MapTaskCh) > 0 {
			task := <-c.MapTaskCh
			*resp = *task
		} else {
			resp.TaskType = WaittingTask
			if c.MapTasks.checkTaskDone() {
				c.MapToReduce()
			}
			return nil
		}
	} else if c.Condition == ReducePhase {
		// TODO
	} else {
		resp.TaskType = NoTask
	}
	
	return nil
}

func (c *Coordinator) ReportTaskStatus(req *Request, resp *Task) error {
	if req.TaskType == MapTask {
		if req.TaskStatus == Finished {
			// Map任务完成
			c.MapTasks.muMap.Lock()
			for _, v := range c.MapTasks.MetaMap {
				if v.Task.TaskID == req.TaskID {
					v.TaskStatus = Finished
					c.MapTasks.muMap.Unlock()
					return nil
				}
			}
			c.MapTasks.muMap.Unlock()
		} else {
			// Map任务失败
			c.MapTasks.muMap.Lock()
			for _, v := range c.MapTasks.MetaMap {
				if v.Task.TaskID == req.TaskID && v.TaskStatus == Running {
					c.MapTaskCh <- v.Task
					v.TaskStatus = Waiting
					c.MapTasks.muMap.Unlock()
					return nil
				}
			}
			c.MapTasks.muMap.Unlock()
		}
	} else {
		// TODO: Reduce Task
	}
	return nil
}

func (c *Coordinator) MapToReduce() {
	if c.Condition == MapPhase {
		c.MapToReduce()
		c.Condition = ReducePhase
	} else if c.Condition == ReducePhase {
		c.Condition = AllDone
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskCh:    make(chan *Task, len(files)),
		ReduceTaskCh: make(chan *Task, nReduce),
		ReducerNum:   nReduce,
		MapNum:       len(files),
		GlobalTaskID: 0,
		MapTasks: TaskCollection{
			MetaMap: make(map[int]*TaskMetaInfo),
		},
	}
	// Your code here.
	c.InitMapTask(files)
	c.server()
	return &c
}

func (c *Coordinator) InitMapTask(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		// fmt.Printf("Task Id: %d\n", id)
		task := Task{
			TaskType:   MapTask,
			FileName:   v,
			TaskID:     id,
			ReducerNum: c.ReducerNum,
		}

		taskMetaInfo := &TaskMetaInfo{
			Task:       &task,
			TaskStatus: Waiting,
		}
		c.MapTasks.AddTask(taskMetaInfo)
		// fmt.Println("Initialize map task :", &task)
		c.MapTaskCh <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.GlobalTaskID
	c.GlobalTaskID++
	return res
}
