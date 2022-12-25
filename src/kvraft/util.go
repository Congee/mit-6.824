package kvraft

import (
	"crypto/rand"
	"math/big"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Counter[K comparable] struct {
	hmap map[K]uint64
}

func NewCounter[K comparable]() Counter[K] {
	return Counter[K]{make(map[K]uint64)}
}

func (c *Counter[K]) Len() int {
	return len(c.hmap)
}

func (c *Counter[K]) Add(key K) {
	if _, ok := c.hmap[key]; ok {
		c.hmap[key]++
	} else {
		c.hmap[key] = 1
	}
}

func (c *Counter[K]) MostCommon() (K, uint64) {
	var key K
	var val uint64 = 0
	for k, v := range c.hmap {
		if v > val {
			key = k
			val = v
		}
	}
	return key, val
}
