package simpledb

import (
	"sort"
	"strconv"
	"sync"
)

// SortedSet commands:
// zadd, zcard, zcount, zincrby, zrange, zrangebysocre, zrank, zrem

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
	data map[string]memberSlice
	len  int
	mu   sync.RWMutex
}

func newSortedSet() *SortedSet {
	return &SortedSet{data: make(map[string]memberSlice, defaultSortedSetSize)}
}

func (s *SortedSet) zAdd(key string, score float64, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		m := zMember{score: score, member: member}
		s.data[key] = append(s.data[key], m)
		s.data[key].Sort()
		s.len++
		return s.len
	}

	m := zMember{score: score, member: member}
	s.data[key] = memberSlice{m}
	s.len++
	return s.len
}

func (s *SortedSet) zCard(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		return len(s.data[key])
	}
	return 0
}

func (s *SortedSet) zCount(key string, min, max float64) int {
	var count int
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		for _, m := range s.data[key] {
			if min <= m.score && m.score <= max {
				count++
			}
		}
	}
	return count
}

func (s *SortedSet) zIncrementBy(key string, increment float64, member string) float64 {

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		// todo fix
		for _, m := range s.data[key] {
			if m.member == member {
				m.score += increment
				return m.score
			}
		}
	}
	s.zAdd(key, increment, member)
	return increment
}

func (s *SortedSet) zRange(key string, start, stop int, withScore bool) memberSlice {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		list memberSlice
	)
	if _, ok := s.data[key]; ok {
		size := len(s.data[key])
		data := s.data[key]
		if stop < 0 {
			stop = size + stop + 1
		}
		for i := start; i < size; i++ {
			if i >= stop {
				break
			}
			list = append(list, data[i])
		}
	}
	return list
}

func (s *SortedSet) zRangeByScore(key string, min, max float64, withScore bool) memberSlice {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		list memberSlice
	)
	if _, ok := s.data[key]; ok {
		for _, m := range s.data[key] {
			if min <= m.score && m.score <= max {
				list = append(list, m)
			}
		}
	}
	return list
}

func (s *SortedSet) zRank(key, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		for i, m := range s.data[key] {
			if m.member == member {
				return i + 1
			}
		}
	}
	return 0
}

func (s *SortedSet) zRem(key string, members ...string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		list memberSlice
	)
	if _, ok := s.data[key]; ok {
		hasMap := make(map[string]zMember)
		for _, m := range s.data[key] {
			hasMap[m.member] = m
		}
		for _, member := range members {
			if _, ok := hasMap[member]; !ok {
				list = append(list, hasMap[member])
			}
		}
		s.data[key] = list
		return true
	}
	return false
}

func zAdd(s *Server, resp *Resp) error {

	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	score, err := strconv.ParseFloat(string(resp.Array[2].Value), 64)
	if err != nil {
		return s.replyErr(err)
	}
	member := string(resp.Array[3].Value)
	size := s.zSet.zAdd(key, score, member)
	return s.writeArgs(size)
}

func zCard(s *Server, resp *Resp) error {
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	size := s.zSet.zCard(key)
	return s.writeArgs(size)

}

func zCount(s *Server, resp *Resp) error {
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	min, err := strconv.ParseFloat(string(resp.Array[2].Value), 64)
	if err != nil {
		return s.replyErr(err)
	}
	max, err := strconv.ParseFloat(string(resp.Array[3].Value), 64)
	if err != nil {
		return s.replyErr(err)
	}
	pos := s.zSet.zCount(key, min, max)
	return s.writeArgs(pos)
}

func zIncrementBy(s *Server, resp *Resp) error {
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	increment, err := strconv.ParseFloat(string(resp.Array[2].Value), 64)
	if err != nil {
		return s.replyErr(err)
	}
	member := string(resp.Array[3].Value)

	curScore := s.zSet.zIncrementBy(key, increment, member)
	return s.writeArgs(curScore)

}

func zRange(s *Server, resp *Resp) error {
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	start, err := strconv.Atoi(string(resp.Array[2].Value))
	if err != nil {
		return s.replyErr(err)
	}
	stop, err := strconv.Atoi(string(resp.Array[3].Value))
	if err != nil {
		return s.replyErr(err)
	}
	result := s.zSet.zRange(key, start, stop, true)
	return s.writeArgs(result)
}

func zRangeByScore(s *Server, resp *Resp) error {
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	min, err := strconv.ParseFloat(string(resp.Array[2].Value), 64)
	if err != nil {
		return s.replyErr(err)
	}
	max, err := strconv.ParseFloat(string(resp.Array[3].Value), 64)
	if err != nil {
		return s.replyErr(err)
	}
	result := s.zSet.zRangeByScore(key, min, max, true)
	return s.writeArgs(result)
}
func zRank(s *Server, resp *Resp) error {
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	member := string(resp.Array[2].Value)

	rank := s.zSet.zRank(key, member)
	return s.writeArgs(rank)
}
func zRem(s *Server, resp *Resp) error {

	var (
		members []string
	)
	if s.zSet == nil {
		s.zSet = newSortedSet()
	}
	key := string(resp.Array[1].Value)
	for _, m := range resp.Array[1:] {
		members = append(members, string(m.Value))
	}
	result := s.zSet.zRem(key, members...)
	return s.writeArgs(result)

}
