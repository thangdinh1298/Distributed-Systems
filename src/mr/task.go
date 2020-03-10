package mr

type taskType int

const (
	mapTask    taskType = 0
	reduceTask taskType = 1
)

// Task interface represents a task
type Task interface {
	GetType() taskType
	GetFileName() string
	Execute() error
}

type MapTask struct {
	TType    taskType
	FileName string
}

type ReduceTask struct {
	TType    taskType
	FileName string
}

func (t MapTask) GetType() taskType {
	return t.TType
}

func (t MapTask) GetFileName() string {
	return t.FileName
}

func (t MapTask) Execute() error {
	return nil
}

func (t ReduceTask) GetType() taskType {
	return t.TType
}

func (t ReduceTask) GetFileName() string {
	return t.FileName
}

func (t ReduceTask) Execute() error {
	return nil
}
