package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	MapTasks    *list.List //Contains map tasks not yet assigned to any workers
	ReduceTasks *list.List //contains reduce tasks not yet assigned to any workers
	Lock        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//This function gets called by worker to mark that the task was completed
//If task has already been completed or TType is not valid, it ignores the call
func (m *Master) MarkDone(args Task, rep *struct{}) error {
	m.Lock.Lock()
	// fmt.Printf("Args is: %+v\n", args)
	var l *list.List
	if args.TType == mapTask {
		l = m.MapTasks
	} else if args.TType == reduceTask {
		l = m.ReduceTasks
	}
	for e := l.Front(); e != nil; e = e.Next() {
		// fmt.Printf("%+v\n", e.Value)
		t := e.Value.(*Task)
		if t.TaskNum == args.TaskNum {
			l.Remove(e)
			break
		}
	}

	m.Lock.Unlock()

	return nil
}

//
// This function returns a task to the caller
// Task could be any of the following types:
//  - Map task
//  - Reduce tasl
// This funtion returns an error if something goes wrong or
// All tasks has been completed
func (m *Master) GetTask(args struct{}, reply *Task) error {

	var l *list.List

	m.Lock.Lock()
	if m.MapTasks.Len() > 0 {
		l = m.MapTasks
	} else if m.ReduceTasks.Len() > 0 {
		l = m.ReduceTasks
	} else {
		//out of task, worker can exit now
		return ErrOutOfTask{}
	}

	front := l.Front()
	t := front.Value.(*Task)
	if time.Now().Sub(t.TimeDispatched) >= 10*time.Second {
		l.Remove(front)
		t.TimeDispatched = time.Now()
		l.PushBack(front.Value)
		reply.TType = t.TType
		reply.Args = t.Args
		reply.TaskNum = t.TaskNum
		reply.NReduce = t.NReduce
		reply.NMap = t.NMap
	} else {
		reply.TType = waitTask
	}

	m.Lock.Unlock()

	// fmt.Printf("%v %T\n", *reply, *reply)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	return m.MapTasks.Len() == 0 && m.ReduceTasks.Len() == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		MapTasks:    list.New(),
		ReduceTasks: list.New(),
	}

	//Insert map tasks
	for idx, file := range files {
		m.MapTasks.PushBack(&Task{
			TType:   mapTask,
			Args:    []string{file},
			TaskNum: idx,
			NReduce: nReduce,
			NMap:    len(files),
		})
	}

	//Insert reduce tasks
	for i := 0; i < nReduce; i++ {
		m.ReduceTasks.PushBack(&Task{
			TType:   reduceTask,
			TaskNum: i,
			NReduce: nReduce,
			NMap:    len(files),
		})
	}

	// Your code here.

	m.server()
	return &m
}
