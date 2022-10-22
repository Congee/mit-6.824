package raft

import "log"
import "strconv"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min[T int | int64](x, y T) T {
	if x < y {
		return x
	}
	return y
}

func max[T int | int64](x, y T) T {
	if x > y {
		return x
	}
	return y
}

func last[T any](slice []T) T {
	return slice[len(slice)-1]
}

func trktime(n int64) string {
	return strconv.Itoa(int(n))[7:]
}

func cmpsign[T int | int64](lhs, rhs T) byte {
	if lhs == rhs {
		return '='
	} else if lhs < rhs {
		return '<'
	} else {
		return '>'
	}
}
