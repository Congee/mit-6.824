package raft

type (
	GetState struct {
		done chan struct {
			term     int
			isleader bool
		}
	}
	ElectionTimeout    struct{}
	BroadcastHeatbeats struct{ empty bool }
	SendAppendEntries  struct {
		srv int
		req *AppendEntriesReq
		rep *AppendEntriesRep
	}
	SentAppendEntries    struct{}
	MakeAppendEntriesReq struct {
		srv   int
		empty bool
		done  chan AppendEntriesReq
	}
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
)
