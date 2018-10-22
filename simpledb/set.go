package simpledb

import "sync"

// set commands:
// sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismenber, smembers, srem, sunion, sunionstore

type sMember struct {
	val map[string]interface{}
}

type Set struct {
	data map[string]*sMember
	len  int
	mu   sync.RWMutex
}

func newSet() *Set {
	return &Set{
		data: make(map[string]*sMember, defaultSetSize),
		len:  0,
		mu:   sync.RWMutex{},
	}
}

func (s *Set) add(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, member := range members {
		if _, ok := s.data[key]; ok {
			s.data[key].val[member] = nil
		} else {
			m := make(map[string]interface{})
			m[member] = nil
			s.data[key] = &sMember{val: m}
		}
	}
	s.len = len(s.data)
	return s.len
}

func (s *Set) card(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		return len(s.data[key].val)
	}
	return 0
}

func (s *Set) diff(key0, key1 string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		list []string
	)
	diffMap := make(map[string]interface{})
	if _, ok := s.data[key0]; ok {
		for k, v := range s.data[key0].val {
			if _, ok := s.data[key1]; ok {
				if _, ok := s.data[key1].val[k]; !ok {
					diffMap[k] = v
				}
			} else {
				diffMap[k] = v
			}
		}
	}

	for k, _ := range diffMap {
		list = append(list, k)
	}
	return list
}

func (s *Set) inter(key0, key1 string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		list []string
	)

	interMap := make(map[string]interface{})
	if _, ok := s.data[key0]; ok {
		for k, v := range s.data[key0].val {
			if _, ok := s.data[key1]; ok {
				if _, ok := s.data[key1].val[k]; ok {
					interMap[k] = v
				}
			}
		}
	}
	for k, _ := range interMap {
		list = append(list, k)
	}
	return list
}

func (s *Set) union(key0, key1 string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		list []string
	)
	unionMap := make(map[string]interface{})
	if _, ok := s.data[key0]; ok {
		for k, v := range s.data[key0].val {
			unionMap[k] = v
		}
	}

	if _, ok := s.data[key1]; ok {
		for k, v := range s.data[key1].val {
			unionMap[k] = v
		}
	}

	for k, _ := range unionMap {
		list = append(list, k)
	}
	return list
}

func (s *Set) sIsMember(key string, member string) bool {

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; ok {
		if _, ok := s.data[key].val[member]; ok {
			return true
		}
	}
	return false
}

func (s *Set) sMembers(key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		members []string
	)
	if _, ok := s.data[key]; ok {
		for k, _ := range s.data[key].val {
			members = append(members, k)
		}
	}
	return members
}

func (s *Set) sRem(key, member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; ok {
		if _, ok := s.data[key].val[member]; ok {
			delete(s.data[key].val, member)
			return true
		}
	}
	return false
}

func sAdd(s *Server, resp *Resp) error {

	if s.set == nil {
		s.set = newSet()
	}

	var (
		members []string
	)
	key := string(resp.Array[1].Value)
	for _, member := range resp.Array[1:] {
		members = append(members, string(member.Value))
	}
	size := s.set.add(key, members...)
	return s.writeArgs(size)
}

func sCard(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)
	size := s.set.card(key)
	return s.writeArgs(size)
}

func sDiff(s *Server, resp *Resp) error {

	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.diff(key0, key1)
	return s.writeArgs(result)
}

func sDiffScore(s *Server, resp *Resp) error {

	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.diff(key0, key1)
	return s.writeArgs(len(result))
}

func sInter(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.inter(key0, key1)
	return s.writeArgs(result)
}

func sInterScore(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.inter(key0, key1)
	return s.writeArgs(len(result))
}

func sUnion(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.union(key0, key1)
	return s.writeArgs(result)
}

func sUnionScore(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.union(key0, key1)
	return s.writeArgs(len(result))
}

func sIsMember(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)
	member := string(resp.Array[2].Value)

	result := s.set.sIsMember(key, member)
	return s.writeArgs(result)
}

func sMembers(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)

	result := s.set.sMembers(key)
	return s.writeArgs(result)
}

func sRem(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)
	member := string(resp.Array[2].Value)

	result := s.set.sRem(key, member)
	return s.writeArgs(result)
}
