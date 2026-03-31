package utils

import (
	"maps"
	"slices"
)

type Set[T comparable] map[T]struct{}

func (s Set[T]) Add(vs ...T) {
	for _, v := range vs {
		s[v] = struct{}{}
	}
}

func (s Set[T]) Has(v T) bool { _, ok := s[v]; return ok }
func (s Set[T]) Values() []T  { return slices.Collect(maps.Keys(s)) }
