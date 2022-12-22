package kvraft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const SessionTimeout = 15 * time.Minute

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Key = string

type Value struct {
	data  string
	index int
}

type Client struct {
	id                  int
	lastInteractionTime time.Time // session expiration
}

type LabCommand struct {
	key   string
	value string
	Op    string
}
type GarbageCollector struct{}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // channel to the state machine
	dead    atomic.Bool        // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastClientId int64
	clients      map[int]Client // <ClientId, Client>
	sm           StateMachine[Key, Value]
	bus          chan any
	killch       chan struct{}
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		assert(msg.CommandValid)
		cmd := msg.Command.(LabCommand)
		switch cmd.Op {
		case "Append":
			if v, ok := kv.sm.get(cmd.key); ok {
				kv.sm.put(cmd.key, Value{v.data + cmd.value, msg.CommandIndex})
				continue
			} // else
			fallthrough
		case "Put":
			kv.sm.put(cmd.key, Value{cmd.value, msg.CommandIndex})
		}
	}
}

func (kv *KVServer) gc() {
	for !kv.killed() {
		kv.mu.Lock()
		for key, clt := range kv.clients {
			if clt.lastInteractionTime.UnixMilli() < time.Now().Add(SessionTimeout).UnixMilli() {
				delete(kv.clients, key)
			}
		}
		kv.mu.Unlock()

		time.Sleep(1 * time.Second)
	}
}

func (kv *KVServer) loop() {
	for !kv.killed() {
		select {
		case <-kv.killch:
			return
		case ev := <-kv.bus:
			if !kv.killed() {
				kv.handle(ev)
			}
		}
	}
}

// TODO: bloomfilter

func (kv *KVServer) handle(ev any) {
	switch ev := ev.(type) {
	case HandleReadReq:
		defer func() { ev.done <- struct{}{} }()
		_, isleader := kv.rf.GetState()
		if !isleader {
			ev.rep.Status = kErrWrongLeader
			kv.rf.GetLeaderId(nil, &ev.rep.LeaderHint)
			ev.rep.ClientId = -1 // FIXME
			return
		}

		if v, ok := kv.sm.get(ev.req.Key); ok {
			ev.rep.Status = kOk
			ev.rep.Value = v.data
		} else {
			ev.rep.Status = kErrNoKey
			ev.rep.LeaderHint = -1 // FIXME
		}

	case HandleWriteReq:
		defer func() { ev.done <- struct{}{} }()
		cmd := LabCommand{ev.req.Key, ev.req.Value, ev.req.Op}
		index, _, isleader := kv.rf.Start(cmd)

		if !isleader {
			ev.rep.Status = kErrWrongLeader
			kv.rf.GetLeaderId(nil, &ev.rep.LeaderHint)
			ev.rep.ClientId = -1 // FIXME
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		select {
		case value, ok := <-kv.sm.wait(ctx, ev.req.Key):
			if !ok { // timeout
				ev.rep.Status = kServerTimeout
				break
			}

			if index == value.index {
				ev.rep.Status = kOk
			} else {
				ev.rep.Status = kErrWrongLeader
				kv.rf.GetLeaderId(nil, &ev.rep.LeaderHint)
				// panicf("ABA problem write key=%v value tried=%v actual=%v", ev.req.Value, value) // FIXME: uid?
			}
		case <-done:
			ev.rep.Status = kOk
		case <-time.After(2 * time.Second):
			ev.rep.Status = kServerTimeout
			cancel()
		}
	}
}

func (kv *KVServer) Read(req *ReadReq, rep *ReadRep) {
	// Your code here.
	done := make(chan struct{})
	kv.bus <- HandleReadReq{req, rep, done}
	<-done
}

func (kv *KVServer) Write(req *WriteReq, rep *WriteRep) {
	done := make(chan struct{})
	kv.bus <- HandleWriteReq{req, rep, done}
	<-done
}

// TODO: concurrency, incl. stale RPCs
// func (kv *KVServer) Register(req *RegisterReq, rep *RegisterRep) {
// 	if _, isleader := kv.rf.GetState(); isleader {
// 		rep.LeaderHint = atomic.AddInt64(&kv.lastClientId, 1)
// 		rep.LeaderHint = kv.me
// 	} else {
// 		rep.Status = kErrWrongLeader
// 	}
// }

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.dead.Store(true)
	close(kv.killch)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	return kv.dead.Load()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killch = make(chan struct{})
	kv.bus = make(chan any, 1024)

	// You may need initialization code here.
	go kv.applier()
	go kv.loop()

	return kv
}
