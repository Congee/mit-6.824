package raft

type LabCommand any

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
	SerialNumber uint64
	Value        LabCommand
}

type StartResponse struct {
	index    int
	term     int
	isleader bool
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
//
// # Flow of a Write Log
//
//	Start() ◄─────────────┐                          state machine
//	   │                  │                              │ ▲
//	   │                  │                              │ │
//	   │                  │                              │ │
//	   │                  │   ┌────────► peer────────┐   │ │
//	   │                  │   │                      │   │ │
//	   ▼                  │   │                      ▼   ▼ │
//	 bus ────────────► handler├────────► peer──────►  leader (committed)
//	                          │                      ▲     │
//	                          │                      │     │
//	                          └────────► peer────────┘     │
//	                                                       ▼
//	                                                    client
func (rf *Raft) Start(write LabCommand) (int, int, bool) {
	// Your code here (2B).

	// $5.3 par. 1
	// 1. write to log
	// 2. wait for kill signal, leadership change, or complete committment
	// 3. return to the client

	// TODO: this Start() actually does not directly interact with a client. In a
	// real world, we need a library to await commitment, and then return to the
	// cilent

	done := make(chan StartResponse)
	rf.fire(WriteRequest{Command{Write, hash(write), write}, done})
	result := <-done

	return result.index, result.term, result.isleader
}

func (rf *Raft) WriteNop() (int, int, bool) {
	return rf.Write(Command{Nop, 0, nil})
}

func (rf *Raft) Write(cmd Command) (index int, term int, isleader bool) {
	index = rf.state.nextIndex[rf.me]       // XXX: in case of leadership change, index maybe stale
	term = int(rf.state.currentTerm.Load()) // XXX: this can be stale before finishing committment
	isleader = rf.role == Leader

	if rf.killed() || !isleader {
		return
	}

	// TODO: does this mean reappearing indices?

	// TODO: idempotence via a hashset or even cuckoo filter
	// `cmd` should provide a unique id. But the test suite provides only a bare
	// integer. We use the integer as a unique identifier. Don't do this in
	// practice.

	// for i := len(rf.state.logs) - 1; i >= 0; i-- {
	// 	if rf.state.logs[i].Command.SerialNumber == hash(cmd.Value) {
	// 		index = i + 1
	// 		rf.dbg(dClient, "duplicate hash(%v)=%v=hash(%v)", cmd.Value, hash(cmd.Value), hash(rf.state.logs[i].Command.Value))
	// 		return
	// 	}
	// }

	rf.state.logs = append(rf.state.logs, Log{int64(term), cmd})
	rf.state.nextIndex[rf.me]++
	rf.state.matchIndex[rf.me]++

	return
}
