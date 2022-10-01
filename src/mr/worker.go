package mr

import "bufio"
import "errors"
import "fmt"
import "hash/fnv"
import "io/ioutil"
import "log"
import "net/rpc"
import "os"
import "sort"
import "strconv"
import "strings"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Mapf = func(string, string) []KeyValue
type Reducef = func(string, []string) string

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

func domap(task AskForTaskReply, mapf Mapf) (err error) {
	log.Printf("Got map task '%v'\n", task.InputFilename)
	ifile, err := os.Open(task.InputFilename)
	if err != nil {
		log.Fatalf("cannot open '%v'", task.InputFilename)
		return
	}
	content, err := ioutil.ReadAll(ifile)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFilename)
		return
	}
	defer ifile.Close()
	kva := mapf(task.InputFilename, string(content))
	// Now kva has [{"a": "1"}, {"a": "1"}, {"b": "1"}, ...]
	// We want [{"a": "1"}, {"a": "1"}], [{"b": "1"}, ...], ...

	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	// mr-pid-reduce_partion_id
	// mr-pid1-1
	// mr-pid1-2
	// mr-pid2-1
	// mr-pid3-1
	// ->
	// mr-pid1-1, mr->pid2-1, mr-pid3-1
	// mr-pid1-2
	pid := os.Getpid()
	var ofilenames []string
	for i, kva := range intermediate {
		ofilename := fmt.Sprintf("mr-%d-%v", pid, i)
		ofilenames = append(ofilenames, ofilename)
		log.Println("file to write", ofilename)
		ofile, err := os.OpenFile(ofilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer ofile.Close()

		for _, kv := range kva {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			if kv.Key == "ABOUT" {
				ofile.Sync()
				log.Println("wtfffffffffffffffffff", ofile.Name())
			}
		}
	}

	log.Printf("Finished map task %v\n", task.InputFilename)
	return CallFinishMapTask(task.InputFilename, ofilenames)
}

func merge(intermediate []KeyValue, ofile *os.File, reducef Reducef) {
	// hashmap := make(map[string]int)
	// for _, kv := range intermediate {
	//   v, ok := hashmap[kv.Key]
	//   if ok {
	//     hashmap[kv.Key] = v +1
	//   } else {
	//     hashmap[kv.Key] = 0
	//   }
	// }
	// tmp := []KeyValue{}
	// for k,v := range hashmap {
	//   tmp = append(tmp, KeyValue{k, strconv.Itoa(v)})
	// }
	// sort.Sort(ByKey(tmp))
	// for _, kv := range tmp {
	//   fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	// }
	// return

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		} // .Value is always "1"
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

var hashset = make(map[int]int)

func doreduce(task AskForTaskReply, reducef Reducef) (err error) {
	log.Printf("Got reduce task %v\n", task.Suffix)
	log.Printf("Got reduce files %v\n", task.ReduceFiles)
	intermediate := []KeyValue{}

	for _, f := range task.ReduceFiles {
		file, err := os.OpenFile(f, os.O_RDONLY, 0644)
		if err != nil {
			log.Fatalf("cannot read %v", file.Name())
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			words := strings.Fields(scanner.Text())
			kv := KeyValue{words[0], words[1]}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	// get the last index from "mr-pid-2"
	// -> "mr-out-2"
	oname := "mr-out-" + strconv.Itoa(task.Suffix)

	tmpfile, err := os.CreateTemp("/tmp", oname)
	if err != nil {
		return fmt.Errorf("error %v os.OpenFile(/tmp/%v)", err.Error(), oname)
	}
	defer tmpfile.Close()

	merge(intermediate, tmpfile, reducef)

	err = os.Rename(tmpfile.Name(), oname)

	log.Printf("Finishing reduce task %v\n", task.Suffix)
	CallFinishReduceTask(task.Suffix)

	return
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf Mapf, reducef Reducef) (err error) {
	// Your worker implementation here.

	// Each worker process will ask the coordinator for a task, read the task's
	// input from one or more files, execute the task, and write the task's
	// output to one or more files.

	for {
		task, err := CallAskForTask()
		if err != nil {
			return err
		}

		switch task.Action {
		case ShouldExit:
			os.Exit(0)
		case Wait:
			log.Println("Sleeping")
			time.Sleep(time.Second)
		case DoMap:
			err = domap(task, mapf)
		case DoReduce:
			err = doreduce(task, reducef)
		}

		if err != nil {
			return err
		}
	}

	// Multiple Worker will run.
	// Registering a Worker does not play well with a coordinator because a
	// Worker does not listen to a coordinator for a task. Worker(s) have to be
	// started externally, not by a coordinator.
	// So, just ask for a task.
}

func CallAskForTask() (rep AskForTaskReply, err error) {
	err = nil
	req := AskForTaskRequest{}
	rep = AskForTaskReply{}
	ok := call("Coordinator.AskForTask", &req, &rep)

	if !ok {
		err = errors.New("failed to AskForMapTask()")
	}
	return
}

func CallFinishMapTask(ifilename string, filenames []string) error {
	req := FinishMapTaskRequest{ifilename, filenames}
	rep := FinishMapTaskReply{}
	ok := call("Coordinator.FinishMapTask", &req, &rep)
	if ok {
		return nil
	} else {
		return fmt.Errorf("failed to CallFinishMapTask(%v)", filenames)
	}
}

func CallFinishReduceTask(idx int) error {
	req := FinishReduceTaskRequest{idx}
	rep := FinishReduceTaskReply{}
	ok := call("Coordinator.FinishReduceTask", &req, &rep)
	if ok {
		return nil
	} else {
		return errors.New("failed to callFinishReduceTask()")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
