package raft

import (
	"bytes"

	"6.824/labgob"
)

type PrevPersisted struct {
	// Cache these for the persister to avoid writing with the same thing
	currentTerm  int64
	votedFor     int
	lastLogIndex int // Log Matching property to keep track of the last log entry
	lastLogTerm  int64

	commitIndex int
	lastApplied int

	lastIncludedIndex int
	lastIncludedTerm  int64
}

type Snapshotted struct {
	// Updated on taking a snapshot. These are used by getLastLogTerm() for
	// building a RequestVote RPC request. getLastLogTerm() would be embarrassing
	// if rf.state.log gets truncated after taking a snapshot in a rare case
	lastIncludedIndex int
	lastIncludedTerm  int64
}

type CrashState struct {
	// Persistent
	Term     int64
	VotedFor int
	Log      []Entry

	// Volatile
	CommitIndex int
	LastApplied int

	// Snapshot metadata
	LastIncludedIndex int
	LastIncludedTerm  int64
	LastConfig        Config
}

func (rf *Raft) makeCrashState() CrashState {
	var votedFor int
	if rf.state.votedFor == nil {
		votedFor = -1
	} else {
		votedFor = *rf.state.votedFor
	}

	return CrashState{
		rf.state.currentTerm.Load(),
		votedFor,
		rf.state.log,

		rf.state.commitIndex,
		rf.state.lastApplied,

		rf.snapshotted.lastIncludedIndex,
		rf.snapshotted.lastIncludedTerm,
		Config{},
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// It shall be called on state changes of currentTerm, votedFor, and logs.
// Specifically, `persist` shall be called on each state change. But we need
// transaction for multi-object state changes. in real world.
//
// not thread-safe
func (rf *Raft) persist() {
	// Your code here (2C).

	lastidx := rf.state.baseidx + len(rf.state.log)
	state := rf.makeCrashState()

	// Do not persist if nothing changes to avoid an expensive write
	if true &&
		rf.persisted.currentTerm == state.Term &&
		rf.persisted.votedFor == state.VotedFor &&
		rf.persisted.lastLogIndex == lastidx &&
		rf.persisted.commitIndex == state.CommitIndex &&
		rf.persisted.lastApplied == state.LastApplied &&
		rf.persisted.lastIncludedIndex == state.LastIncludedIndex {
		rf.dbg(dPersist, "skipped; persisted=%+v state=%+v", rf.persisted, state)
		return
	}

	// Persist to disk
	w := new(bytes.Buffer)
	labgob.NewEncoder(w).Encode(&state)
	rf.persister.SaveRaftState(w.Bytes())

	// Update for the next comparison
	rf.persisted.currentTerm = state.Term
	rf.persisted.votedFor = state.VotedFor
	rf.persisted.lastLogIndex = lastidx
	rf.persisted.lastLogTerm = rf.getLastLogTerm()
	rf.persisted.commitIndex = state.CommitIndex
	rf.persisted.lastApplied = state.LastApplied
	rf.persisted.lastIncludedIndex = state.LastIncludedIndex
	rf.persisted.lastIncludedTerm = state.LastIncludedTerm

	rf.dbg(dPersist, "persisted=%+v state=%v", rf.persisted, rf.state)
}

// TODO: abstract duplicate logic
func (rf *Raft) persistStateSnapshot(snapshot []byte) {
	lastidx := rf.state.baseidx + len(rf.state.log)
	state := rf.makeCrashState()

	// Persist to disk
	w := new(bytes.Buffer)
	labgob.NewEncoder(w).Encode(&state)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)

	// Update for the next comparison
	rf.persisted.currentTerm = state.Term
	rf.persisted.votedFor = state.VotedFor
	rf.persisted.lastLogIndex = lastidx
	rf.persisted.lastLogTerm = rf.getLastLogTerm()
	rf.persisted.commitIndex = state.CommitIndex
	rf.persisted.lastApplied = state.LastApplied
	rf.persisted.lastIncludedIndex = state.LastIncludedIndex
	rf.persisted.lastIncludedTerm = state.LastIncludedTerm

	rf.dbg(dPersist, "persisted=%+v state=%v", rf.persisted, rf.state)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var state CrashState
	if err := d.Decode(&state); err != nil {
		panic(err.Error())
	}

	rf.state.currentTerm.Store(state.Term)
	if state.VotedFor == -1 {
		rf.state.votedFor = nil
	} else {
		rf.state.votedFor = &state.VotedFor
	}
	rf.state.log = state.Log

	rf.state.commitIndex = state.CommitIndex
	rf.state.lastApplied = state.LastApplied

	// In case a machine crashes, reboots, then becomes a Candidate
	rf.persisted.lastIncludedIndex = state.LastIncludedIndex
	rf.persisted.lastIncludedTerm = state.LastIncludedTerm
	rf.snapshotted.lastIncludedIndex = state.LastIncludedIndex
	rf.snapshotted.lastIncludedTerm = state.LastIncludedTerm

	rf.state.baseidx = state.LastIncludedIndex
}
