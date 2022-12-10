package raft

type (
	GetState struct {
		done chan struct {
			term     int
			isleader bool
		}
	}
	ElectionTimeout        struct{ term int64 }
	BroadcastHeatbeats     struct{ empty bool }
	HandleAppendEntriesReq struct {
		req  *AppendEntriesReq
		rep  *AppendEntriesRep
		done chan struct{}
	}
	HandleAppendEntriesRep struct {
		srv  int
		req  *AppendEntriesReq
		rep  *AppendEntriesRep
		done chan AEResponse
	}
	HandleRequestVote struct {
		req  *RequestVoteArgs
		rep  *RequestVoteReply
		done chan struct{}
	}
	TakeSnapshot struct {
		index        int
		stateMachine []byte
		done         chan struct{}
	}
	SendSnapshot struct {
		srv int
		req InstallSnapshotReq
		rep InstallSnapshotRep
	}
	WriteRequest struct {
		cmd  Command
		done chan StartResponse
	}
	ReadRequest       struct{}
	WonElection       struct{}
	TrySetCommitIndex struct{}
	TryApply          struct{}
	ReadStateByTest   struct{ done chan State }
	Thunk             struct {
		fn   func()
		file string
		line int
		done chan struct{}
	}
	RoleChange struct {
		from Role
		to   Role
	}
)
