package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:key`
	Value string `json:value`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapOutFile struct {
	file       *os.File
	mapTask    int
	reduceTask int
}

func (f *MapOutFile) String() string {
	return fmt.Sprintf("mr-%d-%d", f.mapTask, f.reduceTask)
}

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
// this method tries to get a task from master
// execute that task by calling the appropriate function
// and communicate back with master when the task is done
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task, _ := GetTask()
		switch task.TType {
		case mapTask:
			fileName := task.Args[0]
			bytes, err := ioutil.ReadFile(fileName)
			if err != nil {
				os.Exit(1)
			}
			pairs := mapf(fileName, string(bytes))
			err = WriteMapResultToFile(task, pairs)
			if err != nil {
				os.Exit(1)
			}
			task.markDone()

		case reduceTask:
			var pairs *[]KeyValue
			outFileName := fmt.Sprintf("mr-out-%d", task.TaskNum)
			file, err := ioutil.TempFile(".", outFileName)

			if err != nil {
				file.Close()
				continue
			}

			writer, err := reduceResultWriter(file)

			if err != nil {
				file.Close()
				continue
			}

			pairs, err = loadAll(task)

			if err != nil {
				file.Close()
				continue
			}

			//Sort the kv slice
			sort.Sort(ByKey(*pairs))

			//for every key that are the same as the one before, pass it to the reduce function
			for i := 0; i < len(*pairs); {
				j := i + 1
				values := []string{}
				values = append(values, (*pairs)[i].Value)
				for j < len(*pairs) && (*pairs)[j].Key == (*pairs)[i].Key {
					values = append(values, (*pairs)[j].Value)
					j++
				}
				value := reducef((*pairs)[i].Key, values)

				if writer((*pairs)[i].Key, value); err != nil {
					file.Close()
					continue
				}
				i = j
			}
			os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", task.TaskNum))
			task.markDone()
		case waitTask:
			// fmt.Println("Waiting to be assigned a task")
		}
		time.Sleep(1 * time.Second)
	}
}

/*
	This function calls the master to notify that the task has been completed
*/
func (t Task) markDone() {
	call("Master.MarkDone", t, &struct{}{})
}

//Returns a function that writes key-value pairs to the designated output file
func reduceResultWriter(f *os.File) (func(string, string) error, error) {
	return func(key, value string) error {
		_, err := f.WriteString(fmt.Sprintf("%s %s\n", key, value))
		return err
	}, nil
}

/*
	This function loads all key value pairs of relevant intermediate files
	into the memory and return the slice
*/
func loadAll(task Task) (*[]KeyValue, error) {
	pairs := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		m := MapOutFile{
			mapTask:    i,
			reduceTask: task.TaskNum,
		}

		f, err := os.Open(m.String())
		defer f.Close()
		// defer f.Close()
		//If file does not exist, skip it because the map task might not have generated an output file for this reduce task
		if os.IsNotExist(err) {
			continue
		}
		//If other error besides file not exists, just exit
		if err != nil {
			return nil, err
		}

		p, err := loadKeyValuePairs(f)
		if err != nil {
			return nil, err
		}

		pairs = append(pairs, p...)

	}

	return &pairs, nil
}

/*
	This function loads the key value pair from the
	intermediate output file f into a slice and return it.
	This function also closes the file passed to it
	Optimize: return a pointer instead
*/
func loadKeyValuePairs(f *os.File) ([]KeyValue, error) {
	dec := json.NewDecoder(f)
	pairs := []KeyValue{}

	pair := KeyValue{}

	for {
		if err := dec.Decode(&pair); err == io.EOF {
			break
		} else if err != nil {
			// fmt.Println("Error decoding intermediate key")
			return nil, err
		}

		pairs = append(pairs, pair)

	}

	return pairs, nil
}

/*
	For every key value pair, this function
	determine the file indices (mapTaskNum, ReduceTaskNum)
	to which the key belongs.
	If the file with this reduce task index has not been opened
	before, it opens a tempfile for it
	Else, it grabs the file and write the encoded string to it

	After it's done with all the pairs
	This function rename all the temp files to the original file
*/
func WriteMapResultToFile(task Task, pairs []KeyValue) error {
	mapTaskNum := task.TaskNum
	numReduceTask := task.NReduce
	files := make([]*MapOutFile, numReduceTask)

	for _, val := range pairs {
		reduceTaskNum := ihash(val.Key) % numReduceTask

		if files[reduceTaskNum] == nil {
			outFile := MapOutFile{
				mapTask:    mapTaskNum,
				reduceTask: reduceTaskNum,
			}

			f, err := ioutil.TempFile(".", outFile.String())
			defer f.Close()
			outFile.file = f

			if err != nil {
				return err
			}

			files[reduceTaskNum] = &outFile
		}
		outFile := files[reduceTaskNum]
		// fmt.Printf("Grabbing %d %d\n", outFile.mapTask, outFile.reduceTask)

		enc := json.NewEncoder(outFile.file)
		err := enc.Encode(val)
		if err != nil {
			return err
		}
	}

	for _, tFiles := range files {
		err := os.Rename(tFiles.file.Name(), tFiles.String())
		if err != nil {
			return err
		}
	}

	return nil
}

// GetTask gets the task from the master
// and return it. It returns an error if
// the rpc call to master returns an error.
// In which case, the worker program should just abort
func GetTask() (Task, error) {

	var reply Task
	var ok = false
	var retry = 5

	for i := 0; ok == false && i < retry; i++ {
		ok = call("Master.GetTask", &struct{}{}, &reply)

		// Sleep 5s if rpc was unsuccessful
		if !ok && i < retry-1 {
			time.Sleep(5 * time.Second)
		}
	}

	if !ok {
		os.Exit(1)
	}

	// fmt.Printf("%+v", reply)

	return reply, nil
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(fmt.Sprintf("Error during call to %s %s", rpcname, err.Error()))
	return false
}
