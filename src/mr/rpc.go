package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type AskForTaskRequest struct {
}

const (
	DoMap int = iota
	DoReduce
	Wait
	ShouldExit
)

type AskForTaskReply struct {
	Action  int
	NReduce int

	// for mapper
	InputFilename string

	// for reducer
	ReduceFiles []string
	Suffix      int
}

type FinishMapTaskRequest struct {
	InputFilename string
	Filenames     []string
}

type FinishMapTaskReply struct {
}

type FinishReduceTaskRequest struct {
	Index int
}

type FinishReduceTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
