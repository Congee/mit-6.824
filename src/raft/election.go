package raft

import (
	"strings"
	"time"
)

// `resetTimer` shall be called only in three cases according to Figure 2.
// a) Got an `AppendEntries` RPC from the current leader.
// b) Granted vote to another peer.
// c) Started an election.
func (rf *Raft) resetTimer() {
	if !rf.ElectionTimer.Stop() {
		if len(rf.ElectionTimer.C) == 1 {
			<-rf.ElectionTimer.C
		}
	}
	rf.ElectionTimer.Reset(rf.ElectionInterval)

	fname, _ := caller(2)
	fn := last(strings.Split(fname, "."))
	future := time.Now().Add(rf.ElectionInterval)
	rf.dbg(dTimer, "election timer -> %s@%s", trktime(future), fn)

	interval := time.Duration(rf.rand.Int63n(ElectionTimeoutBase.Nanoseconds()))
	rf.ElectionInterval = ElectionTimeoutBase + interval
}

func (rf *Raft) campaign() {
	assertf(rf.role != Leader, "role changed to Leader when campaign()")
	// $5.2
	// • On conversion to candidate, start election:
	//   • Increment currentTerm
	//   • Vote for self
	//   • Reset election timer
	//   • Send RequestVote RPCs to all other servers
	term := rf.state.currentTerm.Load()
	rf.dbg(dVote, "role %s -> %s, term %d -> %d", rf.role, Candidate, term, term+1)
	rf.bus <- RoleChange{rf.role, Candidate}
	// Do the same thing after split vote
	rf.role = Candidate
	rf.state.currentTerm.Add(1)
	rf.voteFor(&rf.me)
	rf.persist()
	rf.resetTimer()

	req := RequestVoteArgs{
		rf.state.currentTerm.Load(), // already incremented once
		rf.me,
		rf.state.baseidx + len(rf.state.log),
		rf.getLastLogTerm(),
	}
	go rf.poll(req, time.After(rf.ElectionInterval)) // either times out or wins
	rf.dbg(dTimer, "poll timer to wait for %v", rf.ElectionInterval)
}

// Repeat until one of the three cases happens
//
//  1. Win
//  2. Split vote
//  3. Another candidate wins and sent a heartbeat
func (rf *Raft) poll(req RequestVoteArgs, timer <-chan time.Time) {
	// $5.5
	// If a follower or candidate crashes, then future RequestVote and
	// AppendEntries RPCs sent to it will fail. Raft handles these failures by
	// retrying indefinitely; if the crashed server restarts, then the RPC will
	// complete successfully.
	//
	// But, we have to handle network partitioning - react as soon as we have
	// enough votes to make a decision. Yet replies shall not be just discarded,
	// especailly from a server that has a higher term.
	reps := make(chan RequestVoteReply)
	defer adrain(reps)

	others := rf.Others()
	rf.dbg(dVote, "shall broadcast to peer %d", others)
	for _, server := range others {
		go func(srv int) {
			rf.dbgt(dVote, req.Term, "-> S%d RequestVote-ing", srv)
			rep := RequestVoteReply{}

			for !rf.sendRequestVote(srv, &req, &rep) {
				term := rf.state.currentTerm.Load()
				if term > req.Term {
					rf.dbgt(dDrop, req.Term, "-> S%d currterm=%v > req.Term=%v", srv, term, req.Term)
					return
				}

				rf.dbgt(dDrop, req.Term, "-> S%d RequestVote; No resp, retrying...", srv)
				time.Sleep(HeartbeatInterval / 2) // XXX: because of capped RPC
			}

			// Drop replis sent from an old term
			if req.Term != rf.state.currentTerm.Load() {
				rf.dbg(dDrop, "rcvd stale RequestVoteReply of req.Term=T%d", req.Term)
				return
			}

			rf.dbgt(dVote, req.Term, "-> S%d RequestVote %s", srv, fmtvote(rep.VoteGranted))

			reps <- rep
		}(server)
	}

	// handle RequestVoteReply
	yea := 1
	nay := 0
loop:
	for {
		select {
		case <-timer:
			// Case 2
			rf.dbg(dTimer, "timer timedout yes yea %d, nay %d", yea, nay)
			return
		case rep := <-reps:
			// Case 3
			term := rf.state.currentTerm.Load()
			if rep.Term > term {
				rf.mainrun(func() {
					rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, rep.Term)
					rf.bus <- RoleChange{rf.role, Follower}
					rf.role = Follower
					rf.state.currentTerm.Store(rep.Term)
					rf.persist()
				})
				return
			}

			rf.dbg(dTimer, "timer timedout no  yea %d, nay %d", yea, nay)
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

	rf.dbg(dVote, "decisive votes +%d -%d / %d", yea, nay, len(rf.peers))
	// Case 1
	// e.g., we have 3 nodes in total and node A is down. Node B receives a vote
	// from node C. Then, Node B received a majority of votes. Remember that it
	// also voted for itself.
	if yea > len(rf.peers)/2 { // 2/2, 2/3, 3/4
		rf.bus <- WonElection{}
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int64
	CandidateId int
	// $5.4.1 Election restriction
	LastLogIndex int
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

	done := make(chan struct{})
	rf.bus <- HandleRequestVote{args, reply, done}
	<-done
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := rf.state.currentTerm.Load()
	rf.dbgt(dVote, term, "<- S%d Vote Request of term %d", args.CandidateId, args.Term)

	*reply = RequestVoteReply{term, false}

	// 1. $5.1 stale term from the sender
	if args.Term < term {
		return
	}

	if args.Term > term {
		rf.state.currentTerm.Store(args.Term)
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, args.Term)
		rf.bus <- RoleChange{rf.role, Follower}
		rf.role = Follower
		rf.voteFor(nil)
		rf.persist()
	}

	grantVote := func() {
		reply.VoteGranted = true
		rf.voteFor(&args.CandidateId) // XXX: shall vote only once in current term
		// $5.2
		// If election timeout elapses without receiving AppendEntries RPC from
		// current leader or granting vote to candidate: convert to candidate
		rf.resetTimer()
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, args.Term)
		rf.bus <- RoleChange{rf.role, Follower}
		rf.role = Follower
		rf.persist()
		rf.dbg(dVote, "granted vote to S%d T%d", args.CandidateId, args.Term)
		rf.dbg(dVote, "state=%v getLastLogTerm()=%d", rf.state, rf.getLastLogTerm())
	}

	// $5.4.1 Election restriction
	//
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
	// / XXX: do not grant vote if already voted for some in the current term
	lastidx := rf.state.baseidx + len(rf.state.log)
	if rf.state.votedFor == nil || *rf.state.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLastLogTerm() {
			grantVote()
		} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= lastidx {
			grantVote()
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
func (rf *Raft) sendRequestVote(
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) getLastLogTerm() int64 {
	s := rf.state

	if s.baseidx == 0 {
		if len(s.log) == 0 {
			return 0
		}
		return last(s.log).Term
	}

	if len(s.log) == 0 {
		return rf.snapshotted.lastIncludedTerm
	}

	return last(s.log).Term
}
