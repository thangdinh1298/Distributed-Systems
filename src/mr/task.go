package mr

type taskType int

const (
	mapTask    taskType = 0
	reduceTask taskType = 1
)

// Task interface represents a task
type Task struct {
	TType   taskType
	Args    []string
	TaskNum int
	NReduce int
}
