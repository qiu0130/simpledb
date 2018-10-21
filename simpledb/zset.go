package simpledb

import (
	"sync"
	"sort"
)

/*
SortedSet commands:
zadd, zcard, zcount, zincrby, zrange, zrangebysocre, zrank, zrem, zremrangebyrank
*/

type zMember struct {
	member string
	score  float64
}

type memberSlice []zMember

func (m memberSlice) Less(i, j int) bool {
	return m[i].score < m[j].score
}

func (m memberSlice) Len() int {
	return len(m)
}

func (m memberSlice) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m memberSlice) Reverse() {
	sort.Reverse(m)
}

func (m memberSlice) Sort() {
	sort.Sort(m)
}

type SortedSet struct {
	value map[string]memberSlice
	len int
	mu sync.RWMutex
}

func newSortedSet() *SortedSet {
	return &SortedSet{value: make(map[string]memberSlice, defaultSortedSetSize)}
}

func (s *SortedSet) zadd(key string, score float64, member string) int {

	if _, ok := s.value[key]; ok {
		m := zMember{score: score, member: member}
		s.value[key] = append(s.value[key], m)
		s.value[key].Sort()
		s.len++
		return s.len
	}

	m := zMember{score: score, member: member}
	s.value[key] = memberSlice{m}
	s.len++
	return s.len
}

func (s *SortedSet) zcard(key string) int {

	if _, ok := s.value[key]; ok {
		return len(s.value[key])
	}
	return 0
}
