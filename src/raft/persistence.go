package raft

import (
	"bytes"

	"6.824/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Specifically, `persist` shall be called on each state change. But there are
// problems:
//  1. multi-object state changes. TODO: not in this lab but do it in reality.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var votedFor int
	if rf.state.votedFor == nil {
		votedFor = -1
	} else {
		votedFor = *rf.state.votedFor
	}

	// Persistent
	e.Encode(rf.state.currentTerm.Load())
	e.Encode(votedFor)
	e.Encode(rf.state.logs)

	// Volatile
	e.Encode(rf.state.commitIndex)
	e.Encode(rf.state.lastApplied)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// Persistent
	var currentTerm int64
	var votedFor int
	var logs []Log

	// Volatile
	var commitIndex int
	var lastApplied int

	var err error
	f := func(e any) bool {
		err = d.Decode(e)
		return err == nil
	}

	if f(&currentTerm) && f(&votedFor) && f(&logs) && f(&commitIndex) && f(&lastApplied) {
		rf.state.currentTerm.Store(currentTerm)
		if votedFor == -1 {
			rf.state.votedFor = nil
		} else {
			rf.state.votedFor = &votedFor
		}
		rf.state.logs = logs

		rf.state.commitIndex = commitIndex
		rf.state.lastApplied = lastApplied
	} else {
		panic(err.Error())
	}
}
