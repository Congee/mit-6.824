package raft

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"sync"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (r Role) String() string {
	return [...]string{"Follower", "PreCandidate", "Candidate", "Leader"}[r]
}

func (cmd Command) GoString() string {
	return fmt.Sprintf(
		"Command{Kind:%v SerialNumber:%x Value:%v}",
		cmd.Kind, cmd.SerialNumber, cmd.Value,
	)
}

func (cmd Command) String() string {
	return fmt.Sprintf("%v", cmd.Value)
}

func (rc RoleChange) String() string {
	return fmt.Sprintf("{from:%v to:%v}", rc.from, rc.to)
}

func (s State) String() string {
	voted := func() any {
		if s.votedFor == nil {
			return "<nil>"
		} else {
			return *s.votedFor
		}
	}()
	return fmt.Sprintf(
		"{currentTerm:%d, votedFor:%v, logs:%+v, commitIndex:%d, lastApplied:%d, nextIndex:%v, matchIndex:%v}",
		s.currentTerm.Load(),
		voted,
		s.log,
		s.commitIndex,
		s.lastApplied,
		s.nextIndex,
		s.matchIndex,
	)
}

func (rf Raft) String() string {
	return fmt.Sprintf(
		"{peers %v, me %d, dead %t, ElectionInterval %v, state %s, role %s, leaderId %d}",
		seq(0, len(rf.peers)),
		rf.me,
		rf.dead.Load(),
		rf.ElectionInterval,
		rf.state,
		rf.role,
		rf.leaderId,
	)
}

func (h HandleAppendEntriesReq) String() string {
	return fmt.Sprintf("{req: %+v, rep: %+v}", h.req, h.rep)
}

func (h HandleRequestVote) String() string {
	return fmt.Sprintf("{req: %+v, rep: %+v}", h.req, h.rep)
}

func (rep AppendEntriesRep) String() string {
	cfterm := func() any {
		if rep.ConflictTerm == nil {
			return "<nil>"
		} else {
			return *rep.ConflictTerm
		}
	}()

	return fmt.Sprintf("{Term:%v Success:%t ConflictIndex:%d ConflictTerm:%v}",
		rep.Term, rep.Success, rep.ConflictIndex, cfterm,
	)
}

func (h HandleAppendEntriesRep) String() string {
	return fmt.Sprintf("{srv: %v, req: %+v, rep: %+v}", h.srv, h.req, h.rep)
}

func (w WriteRequest) String() string {
	return fmt.Sprintf("{cmd: %v}", w.cmd)
}

func (b BroadcastHeatbeats) String() string {
	return fmt.Sprintf("{empty: %t}", b.empty)
}

func (r ReadStateByTest) String() string {
	return ""
}

func (t Thunk) String() string {
	return fmt.Sprintf("{file: %v, line: %v}", last(strings.Split(t.file, "/")), t.line)
}

func min[T int | int64](x, y T) T {
	if x < y {
		return x
	}
	return y
}

func max[T int | int64](x, y T) T {
	if x > y {
		return x
	}
	return y
}

func last[T any](slice []T) T {
	return slice[len(slice)-1]
}

// Returns microseconds padded up to 8 digits since `debugStart`.
func trktime(t time.Time) string {
	return fmt.Sprintf("%08d", int(t.UnixMicro()-debugStart.UnixMicro()))
}

func cmpsign[T int | int64](lhs, rhs T) byte {
	if lhs == rhs {
		return '='
	} else if lhs < rhs {
		return '<'
	} else {
		return '>'
	}
}

type Pair[T, U any] struct {
	First  T
	Second U
}

type Triple[T, U, V any] struct {
	First  T
	Second U
	Third  V
}

type Quadruple[T, U, V, X any] struct {
	First  T
	Second U
	Third  V
	Fourth X
}

// TODO: lockless
type Queue[T any] struct {
	mu   sync.Mutex
	list *list.List
}

func (q *Queue[T]) PushBack(element T) T {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.list.PushBack(element).Value.(T)
}

func (q *Queue[T]) PopFront() T {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.list.Remove(q.list.Front()).(T)
}

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.list.Len()
}

func NewQueue[T any]() Queue[T] {
	return Queue[T]{sync.Mutex{}, list.New()}
}

func hash(o any) uint64 {
	h := fnv.New64()
	fmt.Fprintf(h, "%v", o)
	return h.Sum64()
}

func adrain[T any](ch chan T) {
	go func() {
		for range ch {
		}
	}()
}

func (rf *Raft) Others() []int {
	servers := []int{}
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		servers = append(servers, server)
	}
	return servers
}

func seq(from int, to int) []int {
	array := []int{}
	for i := from; i < to; i++ {
		array = append(array, i)
	}
	return array
}

func fmtvote(vote bool) string {
	if vote {
		return "yea"
	} else {
		return "nay"
	}
}

// Clone returns a copy of the slice.
// The elements are copied using assignment, so this is a shallow clone.
func CloneSlice[S ~[]E, E any](s S) S {
	// Preserve nil in case it matters.
	if s == nil {
		return nil
	}
	return append(S([]E{}), s...)
}

func assert(exp bool, args ...any) {
	if !exp {
		if len(args) > 0 {
			panic(args)
		} else {
			panic("assertion failed")
		}
	}
}
