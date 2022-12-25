package kvraft

import (
	"fmt"
	"log"
	"time"

	"6.824/raft"
)

type Topic = string

const (
	dInfo   Topic = "INFO"
	dWarn   Topic = "WARN"
	dError  Topic = "ERRO"
	dDrop   Topic = "DROP"
	dSnap   Topic = "SNAP"
	dTest   Topic = "TEST"
	dTrace  Topic = "TRCE"
	dClient Topic = "CLNT"
	dServer Topic = "SERV"
)

const Debug = true

func dbg(topic Topic, format string, args ...any) {
	if Debug {
		prefix := fmt.Sprintf("%s %s ", raft.Trktime(time.Now()), topic)
		log.Printf(prefix+format, args...)
	}
}

func (kv *KVServer) dbg(topic Topic, format string, args ...any) {
	dbg(topic, fmt.Sprintf("S%d ", kv.me)+format, args...)
}

func assert(exp bool) {
	if !exp {
		panic("assertion failed")
	}
}

func assertf(exp bool, format string, args ...any) {
	if !exp {
		panic(fmt.Sprintf(format, args...))
	}
}

func panicf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func (ev HandleReadReq) String() string {
	return fmt.Sprintf("{req:%+v rep:%+v}", ev.req, ev.rep)
}

func (ev HandleWriteReq) String() string {
	return fmt.Sprintf("{req:%+v rep:%+v}", ev.req, ev.rep)
}
