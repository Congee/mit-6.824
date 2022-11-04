package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

type AppendEntriesReq struct {
	Term     int64
	LeaderId int
	// Index of log entry immediately preceding new ones, where "new" corresponds
	// to the peer yet to receive log entries as described in nextIndex[]. $5.3
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []Log
	LeaderCommit int // leader's commitIndex
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

	ConflictIndex int
	ConflictTerm  *int64
}

func (rf *Raft) handleAppendEntriesReq(req *AppendEntriesReq, rep *AppendEntriesRep) {
	term := rf.state.currentTerm.Load()
	*rep = AppendEntriesRep{
		term,
		false,
		0,
		nil,
	}

	// 1. $5.1 stale term from the sender.
	//
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and returns to follower state. If the term in the RPC is
	// smaller than the candidate’s current term, then the candidate rejects the
	// RPC and continues in candidate state.
	rf.dbg(dInfo, "rcvd AppendEntriesReq %+v", req)
	if req.Term < term {
		return
	}

	rf.leaderId = req.LeaderId
	if req.Term > term {
		rf.state.currentTerm.Store(req.Term)
		rf.voteFor(nil)
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, req.Term)
		rf.role = Follower
		return
	}

	if rf.role == Candidate {
		// $5.2 If AppendEntries RPC received from new leader: convert to follower
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, req.Term)
		rf.role = Follower
		rf.voteFor(nil)
	}
	rf.state.currentTerm.Store(req.Term)

	// $5.2
	// If election timeout elapses without receiving AppendEntries RPC from
	// current leader or granting vote to candidate: convert to candidate
	if rf.role == Follower && req.LeaderId == rf.leaderId {
		rf.resetTimer()
	}

	if len(req.Entries) == 0 {
		rf.dbg(dLog, "rcvd empty entries <- S%d w/ T%d", req.LeaderId, req.Term)
		// XXX: Do not return here.
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	// matches prevLogTerm with optimization (§5.3)
	// 1 2 2 3 3-4 5    -- Leader
	// 1 2 2 2 2 3 3 3  -- Follower
	//     * - p - -
	if req.PrevLogIndex == 0 {
		// It matches in case of the initial heartbeat
	} else if req.PrevLogIndex > len(rf.state.logs) {
		rep.ConflictIndex = len(rf.state.logs)
		rep.ConflictTerm = nil
		return
	} else if rf.state.logs[req.PrevLogIndex-1].Term != req.PrevLogTerm {
		rep.ConflictTerm = &rf.state.logs[req.PrevLogIndex-1].Term
		for i := req.PrevLogIndex; i >= 1 && rf.state.logs[i-1].Term == *rep.ConflictTerm; i-- {
			rep.ConflictIndex = i
		}
		return
	}

	from := len(req.Entries)
	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3) same index
	// but different terms.
	//
	// 1 1 1 2     -- outdated
	// 1 1 1 3 4   -- new from sender
	// 1 1 1       -- receiver ready
	// 1 1 1 2 2 2 -- receiver mismatch
	//       -----
	// 1 1 1 3 4   -- same
	//       ___
	for idx := len(req.Entries) - 1; idx >= 0; idx-- { // In case of an outdated AppendEntries
		if len(rf.state.logs) <= idx+req.PrevLogIndex { // TODO: optimzie to O(1)
			from = idx
		} else if rf.state.logs[idx+req.PrevLogIndex].Term != req.Entries[idx].Term {
			rf.state.logs = rf.state.logs[:idx+req.PrevLogIndex]
			from = idx
		}
	}

	// 4. Append any new entries not already in the log
	rf.dbg(dLog, "append %v++%v from=%v S%d", rf.state.logs, req.Entries[from:], from, req.LeaderId)
	rf.state.logs = append(CloneSlice(rf.state.logs), req.Entries[from:]...)

	// 5. If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > rf.state.commitIndex {
		rf.dbg(dCommit, "commitIndex <- %v", min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries)))
		rf.state.commitIndex = min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries))
		rf.fire(TryApply{})
	}

	rep.Success = true
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rep *AppendEntriesRep) {
	done := make(chan struct{})
	rf.fire(HandleAppendEntriesReq{req, rep, done})
	<-done
}

// $5.5 Retry indefinitely, Raft RPCs are idempotent
func (rf *Raft) sendAppendEntries(
	server int,
	req *AppendEntriesReq,
	rep *AppendEntriesRep,
) bool {
	return rf.peers[server].Call("Raft.AppendEntries", req, rep)
}

func (rf *Raft) makeAppendEntriesReq(srv int, empty bool) AppendEntriesReq {
	term := rf.state.currentTerm.Load()
	// Index of log entry immediately preceding new ones
	previdx, prevterm := func(nxt int) (int, int64) {
		if len(rf.state.logs) == 0 || nxt == 1 {
			return 0, 0
		}

		return nxt - 1, rf.state.logs[nxt-2].Term
	}(rf.state.nextIndex[srv])

	req := AppendEntriesReq{
		term,
		rf.me,
		previdx,
		prevterm,
		make([]Log, 0),
		rf.state.commitIndex,
	}
	lastidx := len(rf.state.logs)

	// • If last log index ≥ nextIndex for a follower: send AppendEntries RPC
	//   with log entries starting at nextIndex
	//   • If successful: update nextIndex and matchIndex for follower (§5.3)
	//   • If AppendEntries fails because of log inconsistency: decrement
	//     nextIndex and retry (§5.3)
	if !empty && lastidx >= rf.state.nextIndex[srv] {
		// 0 1  2  3
		// 1 1 *1* 2
		//     nxt last
		// No out of bound error here as lastidx(0) >= 1 won't stand
		req.Entries = rf.state.logs[rf.state.nextIndex[srv]-1:]
	}

	return req
}

type AEResponse int

const (
	AEStaleRPC AEResponse = iota
	AEHigherTerm
	AEConflict
	AESuccess
)

// thread-safe via a lock
func (rf *Raft) handleAppendEntriesRep(
	srv int,
	req *AppendEntriesReq,
	rep *AppendEntriesRep,
) AEResponse {
	if req.Term > rep.Term {
		return AESuccess
	}

	term := rf.state.currentTerm.Load()

	// Drop replis sent from an old term
	if req.Term != term {
		rf.dbg(dDrop, "rcvd stale AppendEntriesRep of term %d", term)
		return AEStaleRPC
	}

	// XXX: this can happen if this leader is partitioned and another has won
	if rep.Term > term {
		rf.state.currentTerm.Store(rep.Term)
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, rep.Term)
		rf.role = Follower
		rf.voteFor(nil) // the peer may or may not be a leader
		rf.persist()
		return AEHigherTerm
	}

	if rf.role == Leader { // in case of leadership change but no term change
		// Decrement and retry on inconsistency
		if !rep.Success {
			rf.dbgt(dLeader, term, "-> S%d rep.Failure w/ %v <- T%d nxt=%d rep=%+v", srv, req.Entries, rep.Term, rf.state.nextIndex[srv], rep)

			if rep.ConflictTerm != nil {
				for i := len(rf.state.logs); i >= 1; i-- {
					if rf.state.logs[i-1].Term == *rep.ConflictTerm {
						rf.state.nextIndex[srv] = i + 1
						break
					}
				}
			} else {
				rf.state.nextIndex[srv] = rep.ConflictIndex
			}
			rf.dbgt(dLeader, term, "Conclicting req=%+v after state=%s", req, rf.state)
			return AEConflict
		}
		rf.dbgt(dLeader, term, "-> S%d rep.Success w/ %v <- T%d nxt=%d", srv, req.Entries, rep.Term, rf.state.nextIndex[srv])

		// Successful
		// 1 2 3 4 5
		//
		// 1 1 1 3 4
		// ->  *   *
		// 1 1 1 2 2 2
		// NOTE: doing max() to avoid stale replies
		newidx := req.PrevLogIndex + len(req.Entries)
		rf.state.nextIndex[srv] = max(rf.state.nextIndex[srv], newidx+1)
		rf.state.matchIndex[srv] = max(rf.state.matchIndex[srv], newidx)

		rf.dbg(dLog, "updated for S%d nextIndex=%v matchIndex=%v", srv, rf.state.nextIndex, rf.state.matchIndex)
		return AESuccess
	}
	return AEStaleRPC
}

func (rf *Raft) sendAppendEntriesWrapper(srv int, req AppendEntriesReq) AppendEntriesRep {
	rf.dbgt(dLeader, req.Term, "-> S%d sending AppendEntries %+v", srv, req)

	rep := AppendEntriesRep{}
	for !rf.sendAppendEntries(srv, &req, &rep) && rf.state.currentTerm.Load() == req.Term {
		rf.dbgt(dDrop, req.Term, "-> S%d failed  AppendEntries; retrying...", srv)
		time.Sleep(30 * time.Millisecond) // XXX: because of capped RPCs/s
	}

	rf.dbgt(dLeader, req.Term, "-> S%d sent    AppendEntries", srv)
	return rep
}

// @empty bool:
//
//	true for initial heartbeats after winning election;
//	false means regular heartbeats
func (rf *Raft) broadcastHeartbeats(empty bool) {
	// TODO: cancellation
	queue := NewQueue[int]()
	for _, srv := range rf.Others() {
		queue.PushBack(srv)
	}

	var once sync.Once
	var wg sync.WaitGroup

	success := uint32(0)
	for queue.Len() > 0 {
		srv := queue.PopFront()
		await := make(chan AppendEntriesReq)
		rf.fire(MakeAppendEntriesReq{srv, empty, await})
		req := <-await

		wg.Add(1)
		go func() {
			ch := make(chan AppendEntriesRep)
			go func() { ch <- rf.sendAppendEntriesWrapper(srv, req) }()
			rep := <-ch

			done := make(chan AEResponse)
			rf.fire(HandleAppendEntriesRep{srv, &req, &rep, done})
			resp := <-done

			switch resp {
			case AESuccess:
				if int(atomic.AddUint32(&success, 1)) > len(rf.peers)/2-1 {
					once.Do(func() { rf.fire(TrySetCommitIndex{}) })
				}
			case AEConflict:
				queue.PushBack(srv)
			case AEHigherTerm:
			case AEStaleRPC:
			}

			wg.Done()
		}()
	}

	// NOTE: still needed here because replication can happen across multiple
	// rounds of heartbeats
	rf.fire(TrySetCommitIndex{})

	wg.Wait()
}
