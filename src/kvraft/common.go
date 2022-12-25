package kvraft

type Status uint8

const (
	kOk Status = iota
	kErrNoKey
	kErrWrongLeader
	kNewClient
	kServerTimeout
	kClientTimeout
)

func (s Status) String() string {
	return [...]string{"OK", "ErrNoKey", "ErrWrongLeader"}[s]
}

// XXX: Ahhhhhhhhhhhhhhh I miss Rust so much :(

type SetClientId interface {
	SetClientId(int64)
}

type IRep interface {
	GetStatus() Status
	GetLeaderHint() int
	GetClientId() int64
}

type WriteReq struct {
	Key      string
	Value    string
	Op       string
	ClientId int64
	Seq      uint64
}

type WriteRep struct {
	Status     Status
	LeaderHint int
	ClientId   int64
}

type ReadReq struct {
	Key      string
	ClientId int64
	Seq      uint64
}

type ReadRep struct {
	Value      string
	Status     Status
	LeaderHint int
	ClientId   int64
}

type RegisterReq struct {
	ClientId int64
}

type RegisterRep struct {
	Status     Status
	ClientId   int64
	LeaderHint int
}

func (r *WriteReq) SetClientId(id int64) {
	r.ClientId = id
}

func (r *WriteRep) GetStatus() Status {
	return r.Status
}

func (r *WriteRep) GetLeaderHint() int {
	return r.LeaderHint
}

func (r *WriteRep) GetClientId() int64 {
	return r.ClientId
}

func (r *ReadReq) SetClientId(id int64) {
	r.ClientId = id
}

func (r *ReadRep) GetStatus() Status {
	return r.Status
}

func (r *ReadRep) GetLeaderHint() int {
	return r.LeaderHint
}

func (r *ReadRep) GetClientId() int64 {
	return r.ClientId
}

func (r *RegisterReq) SetClientId(id int64) {
	r.ClientId = id
}

func (r *RegisterRep) GetStatus() Status {
	return r.Status
}

func (r *RegisterRep) GetLeaderHint() int {
	return r.LeaderHint
}

func (r *RegisterRep) GetClientId() int64 {
	return r.ClientId
}
