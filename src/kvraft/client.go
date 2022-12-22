package kvraft

import (
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader     int
	registered chan struct{}
	clientId   int64
}

func (ck *Clerk) keepalive() {
}

func (ck *Clerk) register() {
	// TODO: wait
	counts := make([]int, 0, len(ck.servers))
	ch := make(chan RegisterRep)
	for _, srv := range ck.servers {
		go func() {
			req := RegisterReq{}
			rep := RegisterRep{}
			for !srv.Call("KVServer.RegisterClient", req, rep) {
				time.Sleep(30 * time.Millisecond)
			}
			ch <- rep
		}()
	}

	failed := 0
	for {
		select {
		case rep := <-ch:
			if rep.Status != kOk {
				failed++
				continue
			}

			counts[rep.LeaderHint]++
			if counts[rep.LeaderHint] > len(ck.servers)/2 { // quorum. this srv must be legit
				ck.leader = rep.LeaderHint
				close(ck.registered)
			}
		}
	}

	if failed == len(ck.servers) {
		panic("TODO")
		ck.register()
	} else {
		panic("TODO")
		// no agreement
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// go ck.register()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// TODO: session expired?
	req := ReadReq{key, ck.clientId}
	rep := ReadRep{}
	ck.call("Get", &req, &rep)

	if rep.Status == kErrNoKey {
		assert(rep.Value == "")
	}
	return rep.Value
}

func (ck *Clerk) Put(key string, value string) {
	req := WriteReq{key, value, "Put", ck.clientId}
	rep := WriteRep{}
	ck.call("Write", &req, &rep)
}

func (ck *Clerk) Append(key string, value string) {
	req := WriteReq{key, value, "Append", ck.clientId}
	rep := WriteRep{}
	ck.call("Write", &req, &rep)
}

func (ck *Clerk) call(meth string, req SetClientId, rep IRep) {
	// <-ck.registered

	req.SetClientId(ck.clientId)
	for !ck.servers[ck.leader].Call("KVServer."+meth, req, rep) {
		switch status := rep.GetStatus(); status {
		case kErrWrongLeader:
			ck.leader = rep.GetLeaderHint()
		case kNewClient:
			ck.clientId = rep.GetClientId()
		case kServerTimeout:
			if rep.GetLeaderHint() != ck.leader {
				ck.leader = rep.GetLeaderHint()
			} else {
				// ???
			}
		default:
			return // call succeeded
		}
	}
}
