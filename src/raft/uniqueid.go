package raft

import (
	"time"
)

// golang has no bitfield :(
type SnowflakeId struct {
	lasttime int64
	seq      int
}

func (s *SnowflakeId) generate() int64 {
	// 1 bit    41 bits            5 bits  5 bits           12 bits
	// 0 |   timestamp             |DC   |  M.A.C. |        seq no     |

	var mask int64 = 0x7fffffff_ffffffff ^ 0x3fffff // first 41 bits
	now := time.Now().UnixMilli()
	if now == s.lasttime {
		s.seq += 1
	} else {
		s.seq = 0
	}
	timestamp := (now & mask) >> 1
	return timestamp | int64(s.seq)
}
