package stack

import "sync"

type Stack[T any] struct {
	mu    sync.RWMutex
	items []T
}

func (s *Stack[T]) Len() int {
	return len(s.items)
}

func (s *Stack[T]) Top() T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.items[len(s.items)-1]
}

func (s *Stack[T]) Push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, item)
}

func (s *Stack[T]) Pop() T {
	s.mu.Lock()
	defer s.mu.Unlock()
	top := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return top
}

func New[T any]() Stack[T] {
	return Stack[T]{}
}

func From[T any, U chan T | []T](rangeable U) Stack[T] {
	items := []T{}
	switch xs := any(rangeable).(type) {
	case chan T:
		for item := range xs {
			items = append(items, item)
		}
	case []T:
		items = append(items, xs...)
	}
	return Stack[T]{sync.RWMutex{}, items}
}
