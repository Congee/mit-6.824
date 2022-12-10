package raft

import (
	"context"
	"sync"
)

type InstallSnapshotReq struct {
	Term              int64
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int64
	LastConfig        Config
	Offset            uint64
	Data              []byte
	Done              bool
}

type InstallSnapshotRep struct {
	Term int64
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(
	lastIncludedTerm int,
	lastIncludedIndex int,
	snapshot []byte,
) bool {
	// Your code here (2D).

	// When Raft log size in bytes reaches 4x previous snpashot size
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
// The index argument indicates the highest log entry that's reflected in the
// snapshot. Raft should discard its log entries before that point. You'll need
// to revise your Raft code to operate while storing only the tail of the log.
//
// The stateMachine argument is passed from the state machine for us to make a
// snapshot
func (rf *Raft) Snapshot(index int, stateMachine []byte) {
	// Your code here (2D).

	// 1. Fork state machine's memory; see fork(2)
	//   • In parent, continue processing requests
	//   • In child, serialize in-memory data structure to a new snapshot file on
	//   disk
	// 2. Discard previous snapshot file on disk
	// 3. Discard Raft log up through child's last applied index
	done := make(chan struct{})
	rf.fire(TakeSnapshot{index, stateMachine, done})
	<-done
}

func (rf *Raft) takeSnapshot(index int, stateMachine []byte) {
	if rf.snapshotted.lastIncludedIndex == index {
		return
	}

	entry := rf.state.log[index-1-rf.state.baseidx]

	// 5 [6 7 8]
	//       ^ cut here
	rf.state.log = rf.state.log[index-rf.state.baseidx:]
	rf.state.baseidx = entry.Index
	rf.snapshotted.lastIncludedIndex = entry.Index
	rf.snapshotted.lastIncludedTerm = entry.Term

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), stateMachine)
}

func (rf *Raft) InstallSnapshot(
	req *InstallSnapshotReq,
	rep *InstallSnapshotRep,
) {
	// Usually the snapshot will contain new information not already in the
	// follower’s log. In this case, the follower discards its entire log; it is
	// all superseded by the snapshot and may possibly have uncommitted entries
	// that conflict with the snapshot. If, instead, the follower receives a
	// snapshot that describes a prefix of its log (due to retransmission or by
	// mistake), then log entries covered by the snapshot are deleted but entries
	// following the snapshot are still valid and must be retained. (p52)

	rep.Term = rf.state.currentTerm.Load()

	if req.Term < rep.Term {
		return
	}

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.Snapshot = req.Data
	msg.SnapshotTerm = int(req.LastIncludedTerm)
	msg.SnapshotIndex = req.LastIncludedIndex

	rf.mainrun(func() {
		rf.resetTimer()
		rf.dbg(dSnap, "begin snap persisted=%+v state=%v", rf.persisted, rf.state)

		lastidx := rf.state.baseidx + len(rf.state.log)
		savesnap := false
		assert(rf.state.baseidx == rf.snapshotted.lastIncludedIndex)
		rf.dbg(dSnap, "rcvd InstallSnapshot from S%d req=%+v", req.LeaderId, req)
		// NOTE: don't mess with nextIndex[perr] tracked by the leader
		// NOTE: by the time this InstallSnapshot is called, lastApplied ==
		// commitIndex is always true.
		// NOTE: it is safe to compare with commitIndex because a snapshot is only
		// taken upon commitIndex and LeaderCommit > peer.commitIndex always
		// 2 3 4 [5 6 7 8 9 10 11 12 13 14]
		//     *      *        *
		//     base   app      cmt
		if req.LastIncludedIndex == lastidx && req.LastIncludedTerm == rf.getLastLogTerm() {
			rf.dbg(dSnap, "snapshot == rf.state.log; do nothing")
		} else if req.LastIncludedIndex >= lastidx {
			// apply snapshot and truncate log
			rf.dbg(dSnap, "applyin snapshot Index=%d Term=%d state=%+v", msg.SnapshotIndex, msg.SnapshotTerm, rf.state)
			rf.buffer <- msg
			savesnap = true

			rf.state.log = []Entry{}

			rf.state.baseidx = req.LastIncludedIndex
			rf.state.commitIndex = req.LastIncludedIndex
			rf.state.lastApplied = req.LastIncludedIndex

			rf.snapshotted.lastIncludedIndex = req.LastIncludedIndex
			rf.snapshotted.lastIncludedTerm = req.LastIncludedTerm
			rf.dbg(dSnap, "applied snapshot Index=%d Term=%d state=%+v", msg.SnapshotIndex, msg.SnapshotTerm, rf.state)

		} else if req.LastIncludedIndex > rf.snapshotted.lastIncludedIndex && req.LastIncludedIndex <= rf.state.lastApplied {
			// just trim log
			rf.dbg(dSnap, "trim log=%v -> %v:", rf.state.log, req.LastIncludedIndex-rf.state.baseidx)
			savesnap = true

			rf.state.log = rf.state.log[req.LastIncludedIndex-rf.state.baseidx:]

			rf.state.baseidx = req.LastIncludedIndex
			rf.state.lastApplied = max(rf.state.lastApplied, req.LastIncludedIndex)
			rf.state.commitIndex = max(rf.state.commitIndex, req.LastIncludedIndex)

			rf.snapshotted.lastIncludedIndex = req.LastIncludedIndex
			rf.snapshotted.lastIncludedTerm = req.LastIncludedTerm
			rf.dbg(dSnap, "trimmed state=%v persisted=%+v", rf.state, rf.persisted)

		} else if req.LastIncludedIndex > rf.snapshotted.lastIncludedIndex && req.LastIncludedIndex > rf.state.lastApplied {
			// apply snapshot and trim log
			rf.dbg(dSnap, "trim apply log=%v -> %v:", rf.state.log, req.LastIncludedIndex-rf.state.baseidx)
			rf.buffer <- msg
			savesnap = true

			rf.state.log = rf.state.log[req.LastIncludedIndex-rf.state.baseidx:]

			rf.state.baseidx = req.LastIncludedIndex
			rf.state.lastApplied = max(rf.state.lastApplied, req.LastIncludedIndex)
			rf.state.commitIndex = max(rf.state.commitIndex, req.LastIncludedIndex)

			rf.snapshotted.lastIncludedIndex = req.LastIncludedIndex
			rf.snapshotted.lastIncludedTerm = req.LastIncludedTerm
			rf.dbg(dSnap, "trimmed applied state=%v persisted=%+v", rf.state, rf.persisted)
		}

		rf.persist()
		if savesnap {
			rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), req.Data)
		}
		rf.dbg(dSnap, "end   snap persisted=%+v state=%v", rf.persisted, rf.state)
	})
}

func (rf *Raft) buildSendSnapshot(srv int) SendSnapshot {
	req := InstallSnapshotReq{
		rf.state.currentTerm.Load(),
		rf.me,
		rf.snapshotted.lastIncludedIndex,
		rf.snapshotted.lastIncludedTerm,
		Config{},
		0,
		rf.persister.ReadSnapshot(),
		false,
	}
	rep := InstallSnapshotRep{}

	return SendSnapshot{srv, req, rep}
}

// When to send snapshot?
// the next entry needed in AppendEntries has already been discarded in the
// leader's log
func (rf *Raft) sendInstallSnapshot(
	server int,
	req *InstallSnapshotReq,
	rep *InstallSnapshotRep,
) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", req, rep)
}

func (rf *Raft) doInstallSnapshot(
	ctx context.Context,
	server int,
	req *InstallSnapshotReq,
	rep *InstallSnapshotRep,
) {
	go func() {
		var once sync.Once
		for {
			select {
			case <-ctx.Done():
				return
			default:
				once.Do(func() { rf.dbg(dSnap, "-> S%d InstallSnapshot req=%+v", server, req) })
				if req.Term != rf.state.currentTerm.Load() {
					rf.dbg(dDrop, "-> S%d; req.Term=%d", server, req.Term)
					return
				}

				if rf.sendInstallSnapshot(server, req, rep) {
					rf.dbg(dSnap, "-> S%d sent snapshot req=%+v rep=%+v", server, req, rep)
					rf.mainrun(func() {
						if rep.Term > rf.state.currentTerm.Load() {
							rf.fire(RoleChange{rf.role, Follower})
							rf.role = Follower
							rf.persist()
						} else {
							rf.state.matchIndex[server] = max(rf.state.matchIndex[server], req.LastIncludedIndex)
							rf.state.nextIndex[server] = max(rf.state.nextIndex[server], rf.state.matchIndex[server]+1)
						}
					})
					return
				}
			}
		}
	}()
}
