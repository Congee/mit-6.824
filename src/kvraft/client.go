package kvraft

import (
	"context"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader     int
	registered chan struct{}
	clientId   int64
	seq        uint64
	snowflake  raft.SnowflakeId
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
	ck.snowflake = raft.NewSnowflakeId()
	ck.clientId = ck.snowflake.Generate()
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
	ck.seq++
	req := ReadReq{key, ck.clientId, ck.seq}
	rep := ReadRep{}
	ck.call("Read", &req, &rep)

	if rep.Status == kErrNoKey {
		assert(rep.Value == "")
	}
	return rep.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.seq++
	req := WriteReq{key, value, "Put", ck.clientId, ck.seq}
	rep := WriteRep{}
	ck.call("Write", &req, &rep)
}

func (ck *Clerk) Append(key string, value string) {
	ck.seq++
	req := WriteReq{key, value, "Append", ck.clientId, ck.seq}
	rep := WriteRep{}
	ck.call("Write", &req, &rep)
}

// NOTE: random_handles() renders LeaderHint useless :/
func (ck *Clerk) findleader() int {
	cancels := []context.CancelFunc{}

	for round := 0; round < 10; round++ {
		dbg(dClient, "finding leader round %d", round)
		for _, c := range cancels {
			c()
		}

		counter := NewCounter[int]()
		timer := time.After(2 * time.Millisecond)

		ch := make(chan int, len(ck.servers))
		for srv := range ck.servers {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)
			go func(srv int) {
			loop:
				for {
					select {
					case <-ctx.Done():
						return
					case <-timer:
						return
					default:
						req := ReadReq{"", ck.clientId, 0}
						rep := ReadRep{}
						ok := ck.servers[srv].Call("KVServer.Read", &req, &rep)
						if rep.Status == kOk || rep.Status == kErrNoKey {
							ch <- srv
						}
						if ok {  // TODO
							break loop
						}
					}
				}
			}(srv)
		}

	loop:
		for srv := range ch {
			select {
			case <-timer:
				break loop
			default:
				counter.Add(srv)
				srv, cnt := counter.MostCommon()
				if int(cnt) > len(ck.servers)/2 {
					return srv
				}
			}
		}
	}

	return -1
}

func (ck *Clerk) call(meth string, req SetClientId, rep IRep) {
	// <-ck.registered
	// cases of wrong leader
	// 1. in election
	// 2. partitioned leader -> timeout
	// 2. partitioned follower -> timeout
	// 3. partitioned follower -> candidate -> timeout (ditto)

	req.SetClientId(ck.clientId)
	for {
		dbg(dClient, "broadcast to find a leader")
		if ck.leader == -1 {
			found := ck.findleader()
			if found == -1 {
				panic("could not find a leader")
			}
			ck.leader = found
		}

		select {
		case <-time.After(2 * time.Second):
			panic("well well well")
		default:
			dbg(dClient, "calling S%d/%d req=%+v", ck.leader, len(ck.servers)-1, req)

			if ck.servers[ck.leader].Call("KVServer."+meth, req, rep) {
				dbg(dClient, "call ok w/ S%d/%d req=%+v rep=%+v", ck.leader, len(ck.servers)-1, req, rep)
				switch status := rep.GetStatus(); status {
				case kNewClient:
					ck.clientId = rep.GetClientId()
				case kServerTimeout:
					dbg(dClient, "Client got kServerTimeout")
					fallthrough
				case kErrWrongLeader: // FIXME: exhausted cycles
					// hint := rep.GetLeaderHint()
					// if hint == -1 {
					// 	ck.leader++
					// 	ck.leader = ck.leader % len(ck.servers)
					// } else {
					// 	ck.leader = hint
					// }
					ck.leader = -1
				default:
					return // call succeeded
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
