package kvraft

type HandleReadReq struct {
	req  *ReadReq
	rep  *ReadRep
	done chan struct{}
}

type HandleWriteReq struct {
	req  *WriteReq
	rep  *WriteRep
	done chan struct{}
}
