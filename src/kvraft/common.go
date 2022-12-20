package kvraft

type Status uint8

const (
	OK Status = iota
	ErrNoKey
	ErrWrongLeader
)

func (s Status) String() string {
	return [...]string{"OK", "ErrNoKey", "ErrWrongLeader"}[s]
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
