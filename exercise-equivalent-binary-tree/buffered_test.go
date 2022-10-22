package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/tour/tree"
)

// https://go.dev/tour/concurrency/8

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(root *tree.Tree, ch chan int) {
	if root == nil {
		return
	}

	Walk(root.Left, ch)
	ch <- root.Value
	Walk(root.Right, ch)
}

func WalkWrapper(root *tree.Tree, ch chan int) {
	Walk(root, ch)
	close(ch)
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {
	ch1 := make(chan int)
	ch2 := make(chan int)

	go WalkWrapper(t1, ch1)
	go WalkWrapper(t2, ch2)

	for {
		val1, ok1 := <-ch1
		val2, ok2 := <-ch2

		fmt.Println(val1, val2)
		if ok1 != ok2 || val1 != val2 {
			return false
		}

		if !ok1 {
			break
		}
	}

	return true
}

func TestBuffered(t *testing.T) {
	assert := assert.New(t)

	t.Log(tree.New(1))
	t.Log(tree.New(1))
	t.Log(tree.New(1))
	t.Log(tree.New(1))
	// ((((1 (2)) 3 (4)) 5 ((6) 7 ((8) 9))) 10)
	// ((((1) 2 (3)) 4 (5 (6))) 7 ((8) 9 (10)))
	// ((((((1) 2) 3 (4)) 5 (6)) 7) 8 ((9) 10))
	// ((1 ((((2) 3 (4)) 5) 6)) 7 ((8) 9 (10)))

	assert.True(Same(tree.New(1), tree.New(1)))
	assert.False(Same(tree.New(1), tree.New(2)))
}
