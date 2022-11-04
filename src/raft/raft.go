package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command any) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sync/atomic"
	"time"
	"runtime"
	"reflect"

	"6.824/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      LabCommand
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// must satisfy broadcastTime ≪ electionTimeout ≪ MTBF
const ElectionTimeoutBase = 350 * time.Millisecond // < 10rqs
const HeartbeatInterval = 150 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	killch    chan struct{}       // closed by Kill()
	dead      atomic.Bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	ElectionInterval time.Duration
	ElectionTimer    *time.Timer
	tick             *time.Ticker
	bus              chan any

	state State

	// read/write by 3 contention cases
	role Role // memory_order_seq_cst

	// $5.2
	// If election timeout elapses without receiving AppendEntries RPC from
	// current *leader* or granting vote to candidate: convert to candidate
	//
	// Only updated after receiving *some* AppendEntries RPCs or after winning an
	// election
	leaderId int

	// Received by the tester
	applyCh chan<- ApplyMsg
}

// TODO: Each log entry also has an integer index identifying its position in
// the log.
type Log struct {
	// Used to detect inconsistencies between logs and to ensure some properties
	Term int64
	// <nil> means no-op
	Command Command
}

// Persistent
type State struct {
	// ------------- persistent ------------------
	// Updated on stable storage before responding to RPCs

	// latest term, init to 0, monotonically increases
	//
	// $5.1 If one server's current term is smaller than the other's, then it
	// updates its current term to the larger value. If a candidate or leader
	// discovers that its term is out of date, it immediately reverts to follower
	// state. If a server receives a request with a stale term number, it rejects
	// the request. Specifically, see AppendEntries RPC and RequestVote RPC.
	currentTerm atomic.Int64

	// `candidateId` voted for in current *term*, nullable.
	votedFor *int
	logs     []Log

	// ------------- volatile on all ------------------

	// Index of highest log entry known to be committed (initialized to 0,
	// increases monotonically).
	commitIndex int
	// Index of highest log entry applied to state machine (initialized to 0,
	// increases monotonically). Committed does not mean applied to state
	// machine.
	lastApplied int

	// ------------- volatile on leader ------------------
	// Reinitialized after election

	// For each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int
	// For each server, index of highest log entry known to be replicated on
	// server (initialized to 0, increases monotonically)
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).

	done := make(chan struct {
		term     int
		isleader bool
	})
	rf.fire(GetState{done})
	result := <-done
	return result.term, result.isleader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mainrun(func() { rf.dbg(dLog, "quit w/ rf=%s", rf) })
	close(rf.killch)
	rf.dead.Store(true)
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

func (rf *Raft) fire(ev any) {
	go func() { rf.bus <- ev }()
}

func (rf *Raft) mainrun(thunk func()) {
	pc := reflect.ValueOf(thunk).Pointer()
	file, line := runtime.FuncForPC(pc).FileLine(pc)

	done := make(chan struct{})
	rf.fire(Thunk{thunk, file, line, done})
	<-done
}

// start as a follower ... no communication (election timeout) ... then election

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

	for !rf.killed() {
		select {
		case <-rf.killch:
			// TODO: cancel ongoing goroutines, if any
			for range rf.bus {
			}
			return
		case <-rf.ElectionTimer.C:
			rf.dbg(dTimer, "electiontimeout after %v", rf.ElectionInterval)
			rf.fire(ElectionTimeout{})
		case <-rf.tick.C:
			rf.dbg(dTimer, "tick")
			if rf.role == Leader {
				rf.fire(BroadcastHeatbeats{empty: false})
				rf.resetTimer() // XXX: 4th case to reset the timer
			}
		case ev := <-rf.bus:
			rf.handle(ev)
		}
	}
}

func (rf *Raft) handle(ev any) {
	rf.dbg(dInfo, "event %T%v", ev, ev)

	switch ev := ev.(type) {
	case Thunk:
		ev.fn()
		close(ev.done)

	case GetState:
		ev.done <- struct {
			term     int
			isleader bool
		}{int(rf.state.currentTerm.Load()), rf.role == Leader}

	case WonElection:
		rf.role = Leader
		rf.leaderId = rf.me
		rf.initializeNextMatchIndex()
		rf.fire(BroadcastHeatbeats{empty: true})

	case ElectionTimeout:
		done := make(chan struct{})
		go func() {
			rf.campaign()
			done <- struct{}{}
		}()
		<-done

	case BroadcastHeatbeats:
		go rf.broadcastHeartbeats(ev.empty)
		rf.tick.Reset(HeartbeatInterval)

	case ReadRequest:
	case WriteRequest:
		index, term, isleader := rf.Write(ev.cmd)
		ev.done <- StartResponse{index, term, isleader}

		if isleader {
			rf.fire(BroadcastHeatbeats{empty: false})
		}

	case TrySetCommitIndex:
		if len(rf.trySetCommitIndex()) > 0 {
			rf.fire(TryApply{})
		}

	case TryApply:
		rf.tryApply()

	case MakeAppendEntriesReq:
		ev.done <- rf.makeAppendEntriesReq(ev.srv, ev.empty)

	case HandleAppendEntriesReq:
		rf.handleAppendEntriesReq(ev.req, ev.rep)
		ev.done <- struct{}{}

	case HandleAppendEntriesRep:
		ev.done <- rf.handleAppendEntriesRep(ev.srv, ev.req, ev.rep)

	case HandleRequestVote:
		rf.handleRequestVote(ev.req, ev.rep)
		ev.done <- struct{}{}

	case ReadStateByTest:
		ev.done <- rf.state
	}
}

// If there exists an N such that N > commitIndex, a majority of
// matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
// (§5.3, §5.4).
//
// Returns a sequence of advanced `commitIndex`. E.g., if the `commitIndex`
// goes from 3 to 5, [4,5] will be the result
func (rf *Raft) trySetCommitIndex() []int {
	term := rf.state.currentTerm.Load()
	logs := rf.state.logs

	rf.dbg(
		dCommit,
		"commitIndex=%d lastApplied=%d nextIndex=%v matchIndex=%v logs=%+v",
		rf.state.commitIndex,
		rf.state.lastApplied,
		rf.state.nextIndex,
		rf.state.matchIndex,
		logs,
	)

	for N := len(logs); N >= 1 && logs[N-1].Term == term; N-- {
		if N <= rf.state.commitIndex {
			continue
		}

		count := 0
		for _, mi := range rf.state.matchIndex {
			if mi >= N {
				count++
			}

			if count > len(rf.state.matchIndex)/2 {
				rf.dbg(dCommit, "commitIndex <- %v, lastApplied=%d", N, rf.state.lastApplied)
				old := rf.state.commitIndex
				rf.state.commitIndex = N
				return []int{old + 1, N}
			}
		}
	}
	return []int{}
}

// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied]
// to state machine (§5.3).
func (rf *Raft) tryApply() []int {
	applied := []int{}
	for rf.state.commitIndex > rf.state.lastApplied {
		rf.state.lastApplied++
		cmd := rf.state.logs[rf.state.lastApplied-1].Command
		cmdidx := rf.state.lastApplied
		applied = append(applied, cmdidx)

		msg := ApplyMsg{
			CommandValid: true,
			Command:      cmd.Value,
			CommandIndex: cmdidx,
		}
		// rf.applier.collect(msg)
		rf.applyCh <- msg // XXX: linearizable writes in tests
		rf.dbg(dLog, "applied cmd=%v @ %v", msg.Command, rf.state.lastApplied)
	}
	return applied
}

// $5.3 When a leader "first" comes to power, it initializes all nextIndex
// values to the index just after the last one in its log.
func (rf *Raft) initializeNextMatchIndex() {
	idx := len(rf.state.logs)
	for i := range rf.peers {
		rf.state.nextIndex[i] = idx + 1
		rf.state.matchIndex[i] = 0
	}
	rf.state.matchIndex[rf.me] = idx
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan<- ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	var bytes [8]byte
	cryptorand.Read(bytes[:])
	seed := int64(binary.LittleEndian.Uint64(bytes[:])) ^ int64(rf.me)
	seed = int64(rf.me)
	rand.Seed(seed)
	interval := time.Duration(rand.Intn(int(ElectionTimeoutBase)/1e6)) * time.Millisecond
	rf.dbg(dTimer, "set election timer to %s", trktime(time.Now().Add(interval)))
	rf.ElectionTimer = time.NewTimer(interval)
	rf.ElectionInterval = ElectionTimeoutBase + interval

	rf.tick = time.NewTicker(HeartbeatInterval)
	rf.bus = make(chan any)
	rf.leaderId = -1
	rf.role = Follower
	rf.state.nextIndex = make([]int, len(peers))
	rf.state.matchIndex = make([]int, len(peers))
	rf.killch = make(chan struct{})
	rf.dead.Store(false)
	rf.applyCh = applyCh
	rf.initializeNextMatchIndex()
	// other rf.state members are zero-initialized

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	rf.dbg(dInfo, "startup with seed=%d", seed)

	return rf
}
