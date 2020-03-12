package mr

import "time"

type taskType int

const (
	mapTask    taskType = 0
	reduceTask taskType = 1
	waitTask   taskType = 2
)

// Task interface represents a task
type Task struct {
	TType          taskType
	Args           []string
	TaskNum        int
	NReduce        int
	NMap           int       //number of map tasks
	TimeDispatched time.Time //time that the task was last dispatched
}

type ErrOutOfTask struct {
}

func (e ErrOutOfTask) Error() string {
	return "No more tasks to perform"
}
