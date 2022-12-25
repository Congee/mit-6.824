package kvraft

type Set[E comparable] struct {
	store map[E]struct{}
}

func (s *Set[E]) Contains(e E) bool {
	_, ok := s.store[e]
	return ok
}

func (s *Set[E]) Insert(e E) {
	s.store[e] = struct{}{}
}

func (s *Set[E]) Erase(e E) {
	delete(s.store, e)
}
