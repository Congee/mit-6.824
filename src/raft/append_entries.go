package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/stack"
)

type AppendEntriesReq struct {
	Term     int64
	LeaderId int
	// Index of log entry immediately preceding new ones, where "new" corresponds
	// to the peer yet to receive log entries as described in nextIndex[]. $5.3
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []Entry
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
	rep.Term = term

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
		rf.bus <- RoleChange{rf.role, Follower}
		rf.role = Follower
		rf.persist()
	}

	if rf.role == Candidate {
		// $5.2 If AppendEntries RPC received from new leader: convert to follower
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, req.Term)
		rf.bus <- RoleChange{rf.role, Follower}
		rf.role = Follower
		rf.voteFor(nil)
	}
	rf.state.currentTerm.Store(req.Term)
	// rep.Term = req.Term  TODO
	rf.persist()

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

	baseidx := rf.state.baseidx
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	// matches prevLogTerm with optimization (§5.3)
	// 1 2 2 3 3-4 5    -- Leader
	// 1 2 2 2 2 3 3 3  -- Follower
	//     * - p - -
	if req.PrevLogIndex == baseidx {
		// It matches in case of the initial heartbeat
	} else if req.PrevLogIndex > baseidx+len(rf.state.log) {
		rep.ConflictIndex = max(1, baseidx+len(rf.state.log))
		rep.ConflictTerm = nil
		return
		// base=6 log=[7,8,9]
		// base=9 log=[]
	} else if rf.state.log[req.PrevLogIndex-1-baseidx].Term != req.PrevLogTerm {
		rep.ConflictTerm = &rf.state.log[req.PrevLogIndex-1-baseidx].Term
		for i := req.PrevLogIndex - 1 - baseidx; i >= 0 && rf.state.log[i].Term == *rep.ConflictTerm; i-- {
			rep.ConflictIndex = i + 1 + baseidx
		}
		return
	}

	from := len(req.Entries)
	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)
	//
	// 1 1 1 2     -- outdated
	// 1 1 1 3 4   -- new from sender
	// 1 1 1       -- receiver ready
	// 1 1 1 2 2 2 -- receiver mismatch
	//       -----
	// 1 1 1 3 4   -- same
	//       ___
	for idx := len(req.Entries) - 1; idx >= 0; idx-- { // In case of an outdated AppendEntries
		if baseidx+len(rf.state.log) <= idx+req.PrevLogIndex { // TODO: optimzie to O(1)
			from = idx
		} else if rf.state.log[idx+req.PrevLogIndex-baseidx].Term != req.Entries[idx].Term {
			rf.state.log = rf.state.log[:idx+req.PrevLogIndex-baseidx]
			from = idx
		}
	}

	// 4. Append any new entries not already in the log
	rf.dbg(dLog, "append %v++%v from=%v S%d", rf.state.log, req.Entries[from:], from, req.LeaderId)
	rf.state.log = append(CloneSlice(rf.state.log), req.Entries[from:]...)
	rf.persist()

	// 5. If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > rf.state.commitIndex {
		rf.dbg(dCommit, "commitIndex <- %v", min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries)))
		rf.state.commitIndex = min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries))
		rf.persist()
		rf.tryApply()
	}

	rep.Success = true
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, rep *AppendEntriesRep) {
	done := make(chan struct{})
	rf.bus <- HandleAppendEntriesReq{req, rep, done}
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

// returning nil indicates a snapshot is needed for peer `srv`
func (rf *Raft) makeAppendEntriesReq(srv int, empty bool) *AppendEntriesReq {
	assert(rf.role == Leader)

	req := &AppendEntriesReq{
		rf.state.currentTerm.Load(),
		rf.me,
		rf.state.nextIndex[srv] - 1, // Index of log entry immediately preceding new ones
		0,
		make([]Entry, 0),
		rf.state.commitIndex,
	}

	lastidx := rf.state.baseidx + len(rf.state.log)
	msg := fmt.Sprintf("lastidx=%v nextIndex[S%v]=%v", lastidx, srv, rf.state.nextIndex[srv])

	// NOTE: log compaction may have happened between AppendEntries RPCs
	// 1 [2 3]
	if req.PrevLogIndex == rf.state.baseidx {
		req.PrevLogTerm = rf.persisted.lastLogTerm
	} else if req.PrevLogIndex > rf.state.baseidx {
		req.PrevLogTerm = rf.state.log[req.PrevLogIndex-1-rf.state.baseidx].Term
	} else {
		return nil
	}

	if empty {
		return req
	}

	// • If last log index ≥ nextIndex for a follower: send AppendEntries RPC
	//   with log entries starting at nextIndex
	//   • If successful: update nextIndex and matchIndex for follower (§5.3)
	//   • If AppendEntries fails because of log inconsistency: decrement
	//     nextIndex and retry (§5.3)
	if lastidx >= rf.state.nextIndex[srv] { // TODO: remove redundancy?
		// 0 1  2  3
		// 1 1 *1* 2
		//     nxt last
		// No out of bound error here as lastidx(0) >= 1 won't stand
		assert(rf.role == Leader)
		req.Entries = rf.state.log[rf.state.nextIndex[srv]-1-rf.state.baseidx:]
	} else {
		// 1 [2 3]
		assertf(lastidx+1 == rf.state.nextIndex[srv], msg)
		req.Entries = rf.state.log[rf.state.nextIndex[srv]-1-rf.state.baseidx:]
	}

	return req
}

type AEResponse uint8

const (
	AEStaleRPC AEResponse = iota
	AEHigherTerm
	AEConflict
	AESnapshot
	AESuccess
)

func (rf *Raft) handleAppendEntriesRep(
	srv int,
	req *AppendEntriesReq,
	rep *AppendEntriesRep,
) AEResponse {
	if req.Term > rep.Term {
		return AESuccess
	}

	term := rf.state.currentTerm.Load()

	// XXX: the sender also handles a stale request in a different place. TODO: clean it up
	// Drop replis sent from an old term
	if req.Term != term {
		rf.dbg(dDrop, "rcvd stale AppendEntriesRep of %v", req)
		return AEStaleRPC
	}

	// XXX: this can happen if this leader is partitioned and another has won
	if rep.Term > term {
		rf.state.currentTerm.Store(rep.Term)
		rf.dbg(dTerm, "role %s -> %s, term %d -> %d", rf.role, Follower, term, rep.Term)
		rf.bus <- RoleChange{rf.role, Follower}
		rf.role = Follower
		rf.voteFor(nil) // the peer may or may not be a leader
		rf.persist()
		return AEHigherTerm
	}

	if rf.role == Leader { // in case of leadership change but no term change
		nextidx := &rf.state.nextIndex[srv]
		// Decrement and retry on inconsistency
		if !rep.Success {
			rf.dbgt(dLog, term, "-> S%d rep.Failure w/ %v <- T%d nxt=%d rep=%+v", srv, req.Entries, rep.Term, *nextidx, rep)

			// NOTE: min() & max() in case of stale RPCs that return `ConflictIndex`.
			// Remember that matchIndex[srv] never backs up.
			update := func() bool {
				// 0 0  1 1
				// 0 1 [2 3]
				for i := len(rf.state.log); i >= 1; i-- {
					if rf.state.log[i-1].Term == *rep.ConflictTerm {
						*nextidx = min(*nextidx, rf.state.baseidx+i+1)       // failure then failure with higher idx
						*nextidx = max(*nextidx, rf.state.matchIndex[srv]+1) // success then failure
						return true
					}
				}
				return false // didn't find ConflictTerm, go with ConflictIndex
			}

			if rep.ConflictTerm == nil || !update() {
				*nextidx = min(*nextidx, rep.ConflictIndex)
				*nextidx = max(*nextidx, rf.state.matchIndex[srv]+1)
			}

			rf.dbgt(dLog, term, "Conclicting req=%+v after state=%s", req, rf.state)
			return AEConflict
		}
		rf.dbgt(dLog, term, "-> S%d rep.Success w/ %v <- T%d nxt=%d", srv, req.Entries, rep.Term, *nextidx)

		// Successful
		// 1 2 3 4 5
		//
		// 1 1 1 3 4
		// ->  *   *
		// 1 1 1 2 2 2
		//
		// NOTE: Avoid stale replies
		//
		// nextIndex is calculated upon matchIndex because a stale RPC in the same
		// term. Request one was sent later and succeeded and updated nextIndex &
		// matchIndex based on the len. Request two was sent earlier but failed.
		// nextIndex[S1] regresses if we do max(nextIndex[S1], len(req.Entries))
		// since len(req.Entries) is as stale as this RPC:
		//
		//			newidx := req.PrevLogIndex + len(req.Entries)
		// 			rf.state.nextIndex[srv] = max(rf.state.nextIndex[srv], newidx+1)
		// 			rf.state.matchIndex[srv] = max(rf.state.matchIndex[srv], newidx)
		//
		//   S0 -> S1 {Success,									   } -> matchIndex[S1] = 489, nextIndex[S1] = 490
		//   S0 -> S1 {Failure, ConflictIndex = 424} -> matchIndex[S1] = 489, nextIndex[S1] = 424
		//
		// To avoid regression, calculate nextIndex upon matchIndex. Remember that
		// nextIndex[S1] backs up, matchIndex[S1] does not
		match := max(rf.state.matchIndex[srv], req.PrevLogIndex+len(req.Entries))
		rf.state.matchIndex[srv] = match
		rf.state.nextIndex[srv] = match + 1

		rf.dbg(dLog, "updated for S%d nextIndex=%v matchIndex=%v", srv, rf.state.nextIndex, rf.state.matchIndex)
		return AESuccess
	}
	return AEStaleRPC
}

func (rf *Raft) sendAppendEntriesWrapper(
	ctx context.Context,
	srv int,
	req AppendEntriesReq,
) (AppendEntriesRep, error) {
	rf.dbgt(dLeader, req.Term, "-> S%d sending AppendEntries %+v", srv, req)

	rep := AppendEntriesRep{}
	for {
		select {
		case <-ctx.Done():
			return rep, errors.New("cancelled")
		default:
			term := rf.state.currentTerm.Load()
			if term != req.Term {
				assert(term > req.Term)
				rf.dbg(dDrop, "-> S%d; req.Term=T%v", srv, req.Term)
				return rep, errors.New("old request term")
			}

			if rf.sendAppendEntries(srv, &req, &rep) {
				rf.dbgt(dLeader, req.Term, "-> S%d sent    AppendEntries rep=%+v", srv, rep)
				return rep, nil
			}

			rf.dbgt(dDrop, req.Term, "-> S%d failed  AppendEntries; retrying...", srv)
			time.Sleep(30 * time.Millisecond) // XXX: because of capped RPCs/s
		}
	}
}

// @empty bool:
//
//	true for initial heartbeats after winning election;
//	false means regular heartbeats
func (rf *Raft) broadcastHeartbeats(ctx context.Context, empty bool) {
	var once sync.Once
	var deposed atomic.Bool
	deposed.Store(false)
	success := uint32(1)

	stack := stack.From[int](rf.Others())
	for stack.Len() > 0 && !deposed.Load() {
		srv := stack.Pop()

		__req := rf.makeAppendEntriesReq(srv, empty)
		if __req == nil {
			rf.bus <- rf.buildSendSnapshot(srv)
			continue
		}

		req := *__req
		go func() {
			ch := make(chan AppendEntriesRep)
			go func() {
				if rep, err := rf.sendAppendEntriesWrapper(ctx, srv, req); err != nil {
					close(ch)
				} else {
					ch <- rep
				}
			}()
			rep, ok := <-ch
			if !ok { // cancelled
				return
			}

			done := make(chan AEResponse)
			rf.bus <- HandleAppendEntriesRep{srv, &req, &rep, done}
			resp := <-done

			switch resp {
			case AESuccess:
				if int(atomic.AddUint32(&success, 1)) > len(rf.peers)/2 {
					once.Do(func() { rf.bus <- TrySetCommitIndex{}} )
				}
			case AEConflict:
				stack.Push(srv)
			case AEHigherTerm:
				deposed.Store(true)
			case AEStaleRPC:
			}
		}()
	}

	// NOTE: still needed here because replication can happen across multiple
	// rounds of heartbeats
	rf.bus <- TrySetCommitIndex{}
}
