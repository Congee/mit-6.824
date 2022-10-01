package mr

import "log"
import "net"
import "errors"
import "strconv"
import "os"
import "net/rpc"
import "strings"
import "net/http"
import "sync"
import "time"

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile)
}

type Timestamp = int64
type Pair = struct {
	timestamp Timestamp
	files     []string
}

type Coordinator struct {
	mu              sync.Mutex
	FilesToMap      sync.Map // map[string]struct{}
	LiveMapTasks    sync.Map // map[int]int64
	FilesToReduce   sync.Map // map[int][]string
	LiveReduceTasks sync.Map // map[int]Pair
	nReduce         int
}

func (c *Coordinator) RemainingFilesToMapCount() int {
	count := 0
	c.FilesToMap.Range(func(_ any, _ any) bool { count++; return true })
	return count
}

func (c *Coordinator) RemainingLiveMapTaskCount() int {
	count := 0
	c.LiveMapTasks.Range(func(_ any, _ any) bool { count++; return true })
	return count
}

func (c *Coordinator) RemainingReduceTaskCount() int {
	count := 0
	c.FilesToReduce.Range(func(_ any, _ any) bool { count++; return true })
	return count
}

func (c *Coordinator) RemainingLiveReduceTaskCount() int {
	count := 0
	c.LiveReduceTasks.Range(func(_ any, _ any) bool { count++; return true })
	return count
}

func (c *Coordinator) AllReducersFinished() bool {
	return c.RemainingReduceTaskCount() == 0 && c.RemainingLiveReduceTaskCount() == 0
}

func (c *Coordinator) AllMappersFinished() bool {
	return c.RemainingFilesToMapCount() == 0 && c.RemainingLiveMapTaskCount() == 0
}

func (c *Coordinator) HandleDeadMapWorker() {
	skewed := []string{}
	c.LiveMapTasks.Range(func(f any, timestamp any) bool {
		if time.Now().Unix()-int64(timestamp.(int64)) > 10 {
			skewed = append(skewed, f.(string))
		}
		return true
	})
	for _, f := range skewed {
		c.mu.Lock()
		c.LiveMapTasks.Delete(f)
		c.FilesToMap.Store(f, time.Now().Unix())
		c.mu.Unlock()
	}
}

func (c *Coordinator) HandleDeadReduceWorker() {
	skewed := []int{}
	c.LiveReduceTasks.Range(func(idx any, pair any) bool {
		if time.Now().Unix()-int64(pair.(Pair).timestamp) > 10 {
			skewed = append(skewed, idx.(int))
		}
		return true
	})
	for _, idx := range skewed {
		c.mu.Lock()
		pair, _ := c.LiveReduceTasks.LoadAndDelete(idx)
		c.FilesToReduce.Store(idx, pair.(Pair).files) // FIXME
		c.mu.Unlock()
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskForTask(req *AskForTaskRequest, rep *AskForTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf(
		"\nRemainingFilesToMapCount(%v),\nRemainingLiveMapTaskCount(%v),\nRemainingLiveReduceTaskCount(%v),\nRemainingReduceTaskCount(%v)\n",
		c.RemainingFilesToMapCount(),
		c.RemainingLiveMapTaskCount(),
		c.RemainingLiveReduceTaskCount(),
		c.RemainingReduceTaskCount(),
	)
	if c.RemainingFilesToMapCount() != 0 {
		c.FilesToMap.Range(func(f any, v any) bool {
			log.Printf("distributing map %v", f.(string))
			*rep = AskForTaskReply{DoMap, c.nReduce, f.(string), []string{}, 0}
			c.LiveMapTasks.Store(f.(string), time.Now().Unix())
			c.FilesToMap.Delete(f.(string))
			return false
		})
	} else if c.RemainingLiveMapTaskCount() != 0 {
		*rep = AskForTaskReply{Wait, 0, "", []string{}, 0}
		return nil
	} else if c.RemainingLiveMapTaskCount() == 0 && c.RemainingReduceTaskCount() != 0 {
		c.FilesToReduce.Range(func(idx any, files any) bool {
			log.Printf("distributing reduce task of idx=%v", idx.(int))
			*rep = AskForTaskReply{DoReduce, c.nReduce, "", files.([]string), idx.(int)}
			c.LiveReduceTasks.Store(idx.(int), Pair{time.Now().Unix(), files.([]string)})
			c.FilesToReduce.Delete(idx.(int))
			return false
		})
	} else if c.RemainingLiveMapTaskCount() == 0 && c.RemainingLiveReduceTaskCount() != 0 {
		*rep = AskForTaskReply{Wait, 0, "", []string{}, 0}
		return nil
	} else if c.Done() {
		*rep = AskForTaskReply{ShouldExit, 0, "", []string{}, 0}
		return nil
	}

	return nil
}

func Contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

func (c *Coordinator) FinishMapTask(req *FinishMapTaskRequest, rep *FinishMapTaskReply) error {
	var err error = nil
	// mr-324-1
	// mr-324-2
	// mr-312-2
	// -> {2: [mr-324-2, mr-312-2]}
	//    {1: [mr-324-1]}

	// NOTE: because a worker can be reused and its pid is not unique, duplicate
	// filenames can happen
	for _, fname := range req.Filenames {
		split := strings.Split(fname, "-")
		idx, err := strconv.Atoi(split[len(split)-1])
		if err != nil {
			log.Printf("failed to load %v\n", idx)
			return err
		}
		c.mu.Lock()
		arr, _ := c.FilesToReduce.LoadOrStore(idx, []string{})
		if !Contains(arr.([]string), fname) {
			arr = append(arr.([]string), fname)
		}
		c.FilesToReduce.Store(idx, arr)
		c.FilesToMap.Delete(req.InputFilename)
		c.mu.Unlock()
	}
	c.LiveMapTasks.Delete(req.InputFilename)
	log.Printf("Finished map %v\n", req.InputFilename)
	return err
}

func (c *Coordinator) FinishReduceTask(req *FinishReduceTaskRequest, rep *FinishReduceTaskReply) error {
	c.LiveReduceTasks.Delete(req.Index)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

// The coordinator should notice if a worker hasn't completed its task in a
// reasonable amount of time (for this lab, use ten seconds), and give the same
// task to a different worker.

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.HandleDeadMapWorker()
	c.HandleDeadReduceWorker()
	return c.AllMappersFinished() && c.AllReducersFinished()
}

func FileExists(f string) bool {
	info, err := os.Stat(f)
	return !errors.Is(err, os.ErrNotExist) && !info.IsDir()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{sync.Mutex{}, sync.Map{}, sync.Map{}, sync.Map{}, sync.Map{}, nReduce}
	c.mu.Lock()
	for _, f := range files {
		c.FilesToMap.Store(f, struct{}{})
	}
	c.mu.Unlock()

	c.server()
	return &c
}
