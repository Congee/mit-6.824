package raft

import "fmt"
import "log"
import "os"
import "strconv"
import "time"
import "runtime"

// Stolen from https://blog.josejg.com/debugging-pretty/
type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func dbg(topic logTopic, format string, a ...any) {
	if Debug {
		// time := time.Since(debugStart).Microseconds()
		// time /= 100
		time := time.Now().UnixMilli()
		prefix := fmt.Sprintf("%s %v ", trktime(time), string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func fname() string {
	pc := make([]uintptr, 15)
	n := runtime.Callers(2, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()
	return frame.Function
}

func caller(skip int) (string, int) {
	pc, _, no, ok := runtime.Caller(skip)
	details := runtime.FuncForPC(pc)
	if !ok || details == nil {
		panic(fmt.Sprintf("caller(skip=%d) failed", skip))
	}
	return details.Name(), no
}
