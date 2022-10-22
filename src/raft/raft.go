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
	"fmt"
	"math/rand"
	"strings"
	"time"
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Kind uint8

const (
	Read Kind = iota
	Write
	Nop // $8. each leader commits a blank no-op entry at the start of its term
)

// e.g.
// Command {Nop, struct{}}
// Command {Write, [obj, value]}
// Command {Read, obj}
type Command struct {
	Kind         Kind
	SerialNumber int64
	Directive    any
}

// as each Raft peer becomes aware that successive log entries are
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
	Command      Command
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

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// must satisfy broadcastTime ≪ electionTimeout ≪ MTBF
const ElectionTimeoutBase = 1000 * time.Millisecond // < 10rqs
const HeartbeatInterval = 100 * time.Millisecond

// TODO: make a module
func makeElectionTimeout() time.Duration {
	return ElectionTimeoutBase + time.Duration(rand.Intn(1000))*time.Millisecond
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      atomic.Bool         // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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

	// Used for election timeout and heartbeats. It should be reset on
	// 1) receiving AppendEntries from current leader,
	// 2) granting vote to a candidate,
	// Reset on successfully finishing an AppendEntries RPC.
	lastHeartbeatTime atomic.Int64 // Millisecond
}

type Log struct {
	Index   int64
	Term    int64 // used to detect inconsistencies between logs and to ensure some properties
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

	//
	commitIndex int64
	lastApplied int64

	// ------------- volatile on leader ------------------
	// Reinitialized after election

	nextIndex  []int64
	matchIndex []int64
}

// dbg with term because term may change before and after sending an RPC
func (rf *Raft) dbgt(topic logTopic, term int64, format string, args ...any) {
	prefix := fmt.Sprintf("T%d S%d ", term, rf.me)
	dbg(topic, prefix+format, args...)
}

func (rf *Raft) dbg(topic logTopic, format string, args ...any) {
	rf.dbgt(topic, rf.state.currentTerm.Load(), format, args...)
}

func (rf *Raft) Lock() {
	flag := Debug
	flag = false
	if flag {
		fname, no := caller(2)
		fn := last(strings.Split(fname, "."))
		rf.dbg(dInfo, "acquiring lock@%s#%d", fn, no)
		rf.mu.Lock()
		rf.dbg(dInfo, "acquired  lock@%s#%d", fn, no)
	} else {
		rf.mu.Lock()
	}
}

func (rf *Raft) Unlock() {
	flag := Debug
	flag = false
	if flag {
		fname, no := caller(2)
		fn := last(strings.Split(fname, "."))
		rf.mu.Unlock()
		rf.dbg(dInfo, "released  lock@%s#%d", fn, no)
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) voteFor(srv *int) {
	if srv == nil {
		rf.state.votedFor = nil
	} else {
		candy := *srv
		rf.state.votedFor = &candy
	}
}

func (s *State) getLastLogIndex() int64 {
	if len(s.logs) == 0 {
		return 0
	}
	return last(s.logs).Index
}

func (s *State) getLastLogTerm() int64 {
	if len(s.logs) == 0 {
		return 0
	}
	return last(s.logs).Term
}

func (s *State) getPrevLogIndex() int64 { // TODO
	return 0
}

func (s *State) getPrevLogTerm() int64 { // TODO
	return 0
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.Lock()
	term = int(rf.state.currentTerm.Load())
	isleader = rf.role == Leader
	rf.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesReq struct {
	Term     int64
	LeaderId int
	// index of log entry immediately preceding new ones.
	// where "new ones" means client requests(writes?) in a buffer that havenn't
	// been committed
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []Log
	LeaderCommit int64 // leader's commitIndex
}

type AppendEntriesRep struct {
	// receiver.currentTerm
	Term int64

	// $5.3
	// To bring a follower’s log into consistency with its own, the leader must
	// find the latest log entry where the two logs agree, delete any entries in
	// the follower’s log after that point, and send the follower all of the
	// leader’s entries after that point. All of these actions happen in response
	// to the consistency check performed by AppendEntries RPCs. The leader
	// maintains a nextIndex for each follower, which is the index of the next
	// log entry the leader will send to that follower. When a leader first comes
	// to power, it initializes all nextIndex values to the index just after the
	// last one in its log (11 in Figure 7). If a follower’s log is inconsistent
	// with the leader’s, the AppendEntries consis- tency check will fail in the
	// next AppendEntries RPC. Af- ter a rejection, the leader decrements
	// nextIndex and retries the AppendEntries RPC. Eventually nextIndex will
	// reach a point where the leader and follower logs match. When this happens,
	// AppendEntries will succeed, which removes any conflicting entries in the
	// follower’s log and appends entries from the leader’s log (if any). Once
	// AppendEntries succeeds, the follower’s log is consistent with the
	// leader’s, and it will remain that way for the rest of the term.
	//
	// true if follower contained entry matching PrevLogIndex and PrevLogTerm.
	// specifically, whether a replicate has been committed. It is used to keep
	// track nextIndex[]
	Success bool
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rep *AppendEntriesRep) {
	rf.Lock()
	defer rf.Unlock()

	term := rf.state.currentTerm.Load()
	*rep = AppendEntriesRep{
		term,
		false,
	}

	// 1. $5.1 stale term from the sender.
	//
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and returns to follower state. If the term in the RPC is
	// smaller than the candidate’s current term, then the candidate rejects the
	// RPC and continues in candidate state.
	rf.dbg(dInfo, "rcvd AppendEntries from S%v T%v", req.LeaderId, req.Term)
	if req.Term < term {
		return
	} else if req.Term == term { // Follower & Candidate
		rf.role = Follower
		rf.voteFor(&req.LeaderId)
		rf.leaderId = req.LeaderId
	} else {
		rf.state.currentTerm.Store(req.Term)

		rf.role = Follower
		rf.voteFor(&req.LeaderId)
		rf.leaderId = req.LeaderId
	}

	now := time.Now().UnixMilli()
	// $5.2
	// If election timeout elapses without receiving AppendEntries RPC from
	// current leader or granting vote to candidate: convert to candidate
	if rf.role == Follower && req.LeaderId == rf.leaderId {
		// reset timer
		fn := last(strings.Split(fname(), "."))
		rf.dbg(dTimer, "reset election timer to %s @%v", trktime(now), fn)
		rf.resetTimer()
	}

	// $5.2 If AppendEntries RPC received from new leader: convert to follower
	if rf.role == Candidate && req.LeaderId != rf.leaderId {
		rf.role = Follower
		rf.voteFor(&req.LeaderId)
		rf.leaderId = req.LeaderId
		rf.resetTimer()
	}

	checkPrevLogIndex := func() bool { // TODO
		return false
	}

	// 2.
	if !checkPrevLogIndex() {
		*rep = AppendEntriesRep{
			rf.state.currentTerm.Load(), // TODO: currentTerm?
			false,
		}
		return
	}

	// TODO: optimize
	// 3. handle entry conflicts
	// same index but different terms
loop:
	for i, ours := range rf.state.logs {
		for _, theirs := range req.Entries {
			if ours.Index == theirs.Index && ours.Term != theirs.Term {
				rf.state.logs = rf.state.logs[:i]
				break loop
			}
		}
	}

	// 4.
	// append any new entries not already in the log
	// TODO: optimize with index
	for _, theirs := range req.Entries {
		same := false
		for _, ours := range rf.state.logs {
			if theirs.Index == ours.Index && theirs.Term == ours.Term {
				same = true
				break
			}
		}

		if !same {
			rf.state.logs = append(rf.state.logs, theirs)
		}
	}

	// 5.
	if req.LeaderCommit > rf.state.commitIndex {
		rf.state.commitIndex = min(req.LeaderCommit, last(req.Entries).Index) // TODO: empty entries
	}
}

// $5.5 Retry indefinitely, Raft RPCs are idempotent
func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, rep *AppendEntriesRep) bool {
	return rf.peers[server].Call("Raft.AppendEntries", req, rep)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.Lock()
	defer rf.Unlock()

	term := rf.state.currentTerm.Load()
	rf.dbgt(dVote, term, "<- S%d Vote Request of term %d", args.CandidateId, args.Term)

	*reply = RequestVoteReply{
		term,
		false,
	}

	// 1. $5.1 stale term from the sender
	if args.Term < term {
		return
	}

	grantVote := func() {
		reply.VoteGranted = true
		rf.voteFor(&args.CandidateId) // XXX: shall vote only once in current term
		// $5.2
		// If election timeout elapses without receiving AppendEntries RPC from
		// current leader or granting vote to candidate: convert to candidate
		rf.resetTimer()
		rf.role = Follower
		rf.dbg(dVote, "granted vote to S%d T%d", args.CandidateId, args.Term)
	}

	if args.Term > term {
		rf.dbg(dVote, "role %s -> %s, term %d -> %d", rf.role, Follower, term, args.Term)
		rf.state.currentTerm.Store(args.Term)
		grantVote() // FIXME: grantVote() may be called twice
		rf.role = Follower
		rf.voteFor(&args.CandidateId)
	}

	if rf.role == Candidate || rf.role == Leader {
		return
	}

	// $5.4.1
	// Raft uses the voting process to prevent a candidate from
	// winning an election unless its log contains all committed
	// entries. A candidate must contact a majority of the cluster
	// in order to be elected, which means that every committed
	// entry must be present in at least one of those servers. If the
	// candidate’s log is at least as up-to-date as any other log
	// in that majority (where “up-to-date” is defined precisely
	// below), then it will hold all the committed entries. The
	// RequestVote RPC implements this restriction: the RPC
	// includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	//
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	//
	// If votedFor is null or candidateId, and candidate’s log is at least as
	// up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	//
	/// XXX: do not grant vote if already voted for some in the current term
	if rf.state.votedFor == nil || *rf.state.votedFor == args.CandidateId {
		if args.LastLogTerm != rf.state.getLastLogTerm() {
			if args.LastLogTerm >= rf.state.getLastLogTerm() {
				grantVote()
			}
		} else {
			if args.LastLogIndex >= rf.state.getLastLogIndex() {
				grantVote()
			}
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.dead.Store(true)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return rf.dead.Load()
}

// start as a follower ... no communication (election timeout) ... then election

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

	rf.dbg(dInfo, "rf.loopForElection()")
	go rf.loopForElection()
	rf.dbg(dInfo, "rf.loopHeartbeatsAsLeader()")
	go rf.loopHeartbeatsAsLeader()
	rf.dbg(dInfo, "rf.loopCommandsHandlingAsLeader()")
	go rf.loopCommandsHandlingAsLeader()
}

func (rf *Raft) loopCommandsHandlingAsLeader() { // TODO
	return
	for !rf.killed() {
	}
}

func (rf *Raft) loopForElection() {
	// reduce the chance of multiple candidates upon startup
	time.Sleep(makeElectionTimeout() - ElectionTimeoutBase)

	x := makeElectionTimeout()
	electionInterval := &x
	for !rf.killed() {
		// XXX: do not create a random time to wait in each loop. It converges to
		// wait for the smallest time period.

		rf.beginElectionIfTimeout(electionInterval)
		// The interval does not have to be the election timeout. We can sleep
		// multiple times.
		// put at the end to start election immediately upon invocation of ticker()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) loopHeartbeatsAsLeader() {
	for !rf.killed() {
		rf.broadcastHeartbeatsIfHeartbeatTimeoutAsLeader()
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) resetTimer() {
	rf.lastHeartbeatTime.Store(time.Now().UnixMilli())
}

func (rf *Raft) hasTimedOut(electionInterval time.Duration) (timeout bool) {
	since := rf.lastHeartbeatTime.Load()
	interval := electionInterval.Milliseconds()
	now := time.Now().UnixMilli()
	timeout = since+interval < now

	if Debug {
		sign := string(cmpsign(since+interval, now))
		rf.dbg(
			dTimer,
			"%v timeout %t %v+%v %s %v",
			rf.role,
			timeout,
			trktime(since),
			interval,
			sign,
			trktime(now),
		)
	}

	return timeout
}

func (rf *Raft) beginElectionIfTimeout(electionInterval *time.Duration) {
	// XXX: contends with another thread that handles AppendEntries RPC
	// XXX: currentTerm could have been incremented here e.g.
	// Another candidate won and currentTerm was mutated Candidate -> Follower
	// Also, an AppendEntries received could also have changed rf.role
	// But, it does not matter as the timer was reset as well.
	rf.Lock()

	if rf.role == Leader {
		rf.Unlock()
		return
	}

	// Follower timeout: no AppendEntries from current leader or granting vote to
	// a candidate.
	// Candidate timeout: split vote happened.
	//
	// So, a Follower resets the election timer upon granting a vote, but a
	// Candidate may not grant a vote upon receiving a RequestVote RPC. Actually,
	// if either receiving an AppendEntries RPC from the current leader or
	// granting vote to a candidate happens, there won't be split vote.
	//
	// It is safe to use only the Follower timeout.
	if !rf.hasTimedOut(*electionInterval) {
		rf.Unlock()
		return
	}

	// $5.2
	// • On conversion to candidate, start election:
	//   • Increment currentTerm
	//   • Vote for self
	//   • Reset election timer
	//   • Send RequestVote RPCs to all other servers
	term := rf.state.currentTerm.Load()
	rf.dbg(dVote, "role %s -> %s, term %d -> %d", rf.role, Candidate, term, term+1)
	// Do the same thing after split vote
	rf.state.currentTerm.Add(1)
	rf.role = Candidate
	rf.voteFor(&rf.me)
	rf.resetTimer()

	*electionInterval = makeElectionTimeout()
	rf.Unlock()

	// XXX: still AppendEntries RPC could cause change to role and currentTerm
	electionStartTime := time.Now().UnixMilli()
	delta := electionStartTime + electionInterval.Milliseconds() - time.Now().UnixMilli()
	pollTimer := time.After(time.Duration(delta) * time.Millisecond) // it is ok for delta < 0
	rf.dbg(dTimer, "timer to wait for %v", time.Duration(delta)*time.Millisecond)

	// repeat until one of the three cases happens
	// case 1: wins
	// case 2: split vote
	// case 3: another candidate should win
	var pollTimeout bool
	for {
		rf.Lock()
		// Case 3
		// Another candidate wins and sent a heartbeat
		if rf.role == Follower {
			rf.Unlock()
			return
		}
		rf.Unlock()

		// Case 2 split vote
		pollTimeout = rf.poll(pollTimer) // rf.role will be set to Follower in case 3
		if pollTimeout {
			return
		}

		rf.Lock()
		if rf.role == Leader {
			rf.Unlock()
			break
		}

		rf.Unlock()
	}

	// As Leader, resetTimer() will happen upon sendAppendEntries.
	// As Follower, reset right away here to reduce contention.
	rf.resetTimer()

	// Case 1
	// Thread-safe as once rf.role is set to Leader, AppendEntries RPCs from
	// other nodes cannot change rf.role. There won't be a RequestVote RPC with
	// ridicuously high term due to network partitioning. Because, we have the
	// majority rule. Simply, we have the Election Safety proprty.
	rf.Lock()
	if rf.role == Leader {
		rf.dbg(dVote, "won")
		rf.leaderId = rf.me
		rf.Unlock()
		rf.broadcastHeartbeatsAfterWinning()
	} else {
		rf.Unlock()
	}
}

func (rf *Raft) handleHigherTerm(peerterm int64, peer int) {
	rf.Lock()
	defer rf.Unlock()
	currterm := rf.state.currentTerm.Load()
	if peerterm > currterm {
		rf.state.currentTerm.Store(peerterm)
		rf.role = Follower
		rf.voteFor(nil)
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, currterm, peerterm)
	}
}

// XXX: can mutate rf.role
func (rf *Raft) poll(timer <-chan time.Time) bool {
	rf.dbg(dVote, "poll()")

	// $5.5
	// If a follower or candidate crashes, then fu- ture RequestVote and
	// AppendEntries RPCs sent to it will fail. Raft handles these failures by
	// retrying indefinitely; if the crashed server restarts, then the RPC will
	// complete successfully.
	//
	// But, we have to handle network partitioning - react as soon as we have
	// enough votes to make a decision. Yet replies shall not be just discarded,
	// especailly from a server that has a higher term.
	reps := make(chan RequestVoteReply)
	go rf.broadcastRequestVote(reps)

	yea := 1
	nay := 0
loop:
	for {
		select {
		case <-timer:
			rf.dbg(dTimer, "timer timedout yes yea %d, nay %d", yea, nay)
			return true
		case rep, ok := <-reps:
			rf.dbg(dTimer, "timer timedout no  yea %d, nay %d", yea, nay)
			if !ok {
				break loop
			}
			if rep.VoteGranted {
				yea += 1
			} else {
				nay += 1
			}

			rf.dbg(dTimer, "votes yea %d, nay %d", yea, nay)
			if max(yea, nay) > len(rf.peers)/2 {
				break loop
			}
		}
	}
	// May have become a Follower now

	rf.dbg(dVote, "decisive votes +%d -%d", yea, nay)
	// e.g., we have 3 nodes in total and node A is down. Node B receives a vote
	// from node C. Then, Node B received a majority of votes. Remember that it
	// also voted for itself.
	rf.Lock()
	defer rf.Unlock()
	// XXX: rf.role might be changed into Follower by handleHigherTerm
	if rf.role == Candidate && yea > len(rf.peers)/2 { // 2/2, 2/3, 3/4
		rf.role = Leader
		rf.dbg(dVote, "Got decisive votes +%d/%d", yea, len(rf.peers))
	}
	return false
}

func (rf *Raft) broadcastRequestVote(reps chan<- RequestVoteReply) {
	term := rf.state.currentTerm.Load()
	rf.Lock()
	req := RequestVoteArgs{
		term, // already incremented once
		rf.me,
		rf.state.getLastLogIndex(),
		rf.state.getLastLogTerm(),
	}
	rf.Unlock()

	rf.dbgt(dVote, term, " %d peers to broadcast", len(rf.peers)-1)
	var wg sync.WaitGroup
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		rf.dbgt(dVote, term, " shall broadcast to peer %d", server)
		wg.Add(1)
		go func(srv int) {
			rf.dbgt(dVote, term, "-> S%d RequestVote", srv)
			rep := RequestVoteReply{}
			rf.sendRequestVote(srv, &req, &rep)
			// FIXME: retry indefinitely in real world
			// for !rf.sendRequestVote(srv, &req, &rep) { // XXX: causes election timeout
			// 	// rf.dbgt(dDrop, term, "-> S%d RequestVote; No resp, retrying...", srv)
			// }
			rf.dbgt(dVote, term, "<- S%d Got vote %t", srv, rep.VoteGranted)

			rf.handleHigherTerm(rep.Term, srv)
			reps <- rep
			wg.Done()
		}(server)
	}
	wg.Wait()
	close(reps)
}

func (rf *Raft) broadcastHeartbeatsIfHeartbeatTimeoutAsLeader() {
	if _, isleader := rf.GetState(); !isleader {
		return
	}

	// NOTE: broadcastHeartbeats shall not block.
	// A peer may be quick to receive an RPC but slow to respond. If we wait
	// for it to respond before sending the next heartbeat, it may timeout.
	go rf.broadcastHeartbeats()
}

// thread-safe
func (rf *Raft) broadcastHeartbeatsAfterWinning() {
	if _, isleader := rf.GetState(); !isleader {
		return
	}
	// No data race here
	//
	// It is impossible for another RequestVoteRPC comes with a higher term
	// because, 1) An RPC call will retry indefinitely if a receiver does not
	// respond. 2) rf.role would already have been demoted to Follower given the
	// receiver had a higher term.
	go rf.broadcastHeartbeats()
}

func (rf *Raft) broadcastHeartbeats() {
	rf.Lock()
	if rf.role != Leader {
		rf.Unlock()
		return
	}

	term := rf.state.currentTerm.Load()
	req := AppendEntriesReq{
		term,
		rf.me,
		rf.state.getPrevLogIndex(),
		rf.state.getPrevLogTerm(),
		make([]Log, 0),
		rf.state.commitIndex,
	}

	reps := make(chan AppendEntriesRep)
	var wg sync.WaitGroup

	// in a new term
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		wg.Add(1)
		go func(srv int) {
			rep := AppendEntriesRep{}
			rf.dbgt(dLeader, term, "-> S%d sending AppendEntries", srv)
			rf.sendAppendEntries(srv, &req, &rep)
			// FIXME: retry indefinitely in real world
			// for !rf.sendAppendEntries(srv, &req, &rep) {
			// 	// rf.dbgt(dDrop, term, "-> S%d failed  AppendEntries; retrying...", srv)
			// }
			rf.dbgt(dLeader, term, "-> S%d sent    AppendEntries", srv)
			// XXX: this can happen if this leader is partitioned and another has won
			rf.handleHigherTerm(rep.Term, srv)
			reps <- rep
			wg.Done()
		}(server)
	}

	rf.Unlock()
	rf.resetTimer()
	wg.Wait()

	// $5.3 After a rejection, the leader decrements nextIndex and retries the
	// AppendEntries RPC. Eventually nextIndex will reach a point where the
	// leader and follower logs match. When this happens, AppendEntries will
	// succeed, which removes any conflicting entries in the follower's log and
	// appends entries from the leader's log (if any). Once AppendEntries
	// succeeds, the follower's log is consistent with the leader's, and it will
	// remain that way for the rest of them.
	for rep := range reps {
		// TODO: mess with nextIndex[]
		// rep.Success
		var _ = rep
	}
}

// $5.3 When a leader "first" comes to power, it initializes all nextIndex
// values to the index just after the last one in its log.
// TODO: what does first mean?
// func (rf *Raft) initializeNextIndex() {
// 	rf.Lock()
// 	idx := rf.state.getLastLogIndex()
// 	for i := range rf.state.nextIndex {
// 		rf.state.nextIndex[i] = idx
// 	}
// 	rf.Unlock()
// }

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1
	rf.role = Follower
	rf.state.nextIndex = make([]int64, len(peers))
	rf.state.matchIndex = make([]int64, len(peers))
	rf.resetTimer()
	for i := range peers {
		rf.state.nextIndex[i] = 1
		rf.state.matchIndex[i] = 0
	}
	// rf.state.logs = []Log{{1, 0, Command{Nop, struct{}{}}}}
	// other rf.state members are zero-initialized

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
