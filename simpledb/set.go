package simpledb

import "sync"

/*
Set commands:
sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismenber, smembers, srem, sunion, sunionstore
*/

type member struct {
	val map[string]struct{}
}

type Set struct {
	value map[string]*member
	len   int
	mu sync.RWMutex
}

func newSet() *Set {
	return &Set{
		value: make(map[string]*member, defaultSetSize),
		len: 0,
		mu: sync.RWMutex{},
	}
}

func (s *Set) add(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, member := range members {
		if v, ok := s.value[key]; ok {
			v.val[member] = struct{}{}
		} else {
			m := make(map[string]struct{})
			m[member] = struct{}{}
			s.value[key] = &member{val: m}
		}
	}
	s.len = len(members)
	return s.len
}

func (s *Set) card(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.len
}

func (s *Set) diff(key0, key1 string) map[string]struct{} {
	diffMap := make(map[string]struct{})

	if _, ok := s.value[key0]; ok {
		for k, v := range s.value[key0].val {
			if _, ok := s.value[key1]; ok {
				if _, ok := s.value[key1].val[k]; !ok {
					diffMap[k] = v
				}
			} else {
				diffMap[k] = v
			}
		}
	}
	return diffMap
}

func (s *Set) inter(key0, key1 string) map[string]struct{} {
	interMap := make(map[string]struct{})

	if _, ok := s.value[key0]; ok {
		for k, v := range s.value[key0].val {
			if _, ok := s.value[key1]; ok {
				if _, ok := s.value[key1].val[k]; ok {
					interMap[k] = v
				}
			}
		}
	}
	return interMap
}

func (s *Set) union(key0, key1 string) map[string]struct{} {

	unionMap := make(map[string]struct{})
	if _, ok := s.value[key0]; ok {
		for k, v := range s.value[key0].val {
			unionMap[k] = v
       }
    }

	if _, ok := s.value[key1]; ok {
		for k, v := range s.value[key1].val {
			unionMap[k] = v
		}
	}
	return unionMap
}

func (s *Set) sismember(key string, member string) bool {

	if _, ok := s.value[key]; ok {
		if _, ok := s.value[key].val[member]; ok {
			return true
		}
	}
	return false
}


func (s *Set) smembers(key string) []string {
	var (
		members []string
	)
	if _, ok := s.value[key]; ok {
		for k, _ := range s.value[key].val {
			members = append(members, k)
		}
	}
	return members
}

func (s *Set) srem(key, member string) bool {
	if _, ok := s.value[key]; ok {
		if _, ok := s.value[key].val[member]; ok {
			delete(s.value[key].val, member)
			return true
		}
	}
	return false
}


func sadd(s *Server, resp *Resp) error {
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
	s.set.add(key, members...)
	return s.replyOk()
}

func scard(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)
	card := s.set.card(key)
	return s.writeArgs(card)
}

func sdiff(s *Server, resp *Resp) error {

	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.diff(key0, key1)
	return s.writeArgs(result)
}

func sdiffscore(s *Server, resp *Resp) error {

	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.diff(key0, key1)
	return s.writeArgs(len(result))
}

func sinter(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.inter(key0, key1)
	return s.writeArgs(result)
}

func sinterscore(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.inter(key0, key1)
	return s.writeArgs(len(result))
}


func sunion(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.union(key0, key1)
	return s.writeArgs(result)
}

func sunionscore(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key0 := string(resp.Array[1].Value)
	key1 := string(resp.Array[2].Value)

	result := s.set.union(key0, key1)
	return s.writeArgs(len(result))
}

func sismember(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)
	member := string(resp.Array[2].Value)

	result := s.set.sismember(key, member)
	return s.writeArgs(result)
}

func smembers(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)

	result := s.set.smembers(key)
	return s.writeArgs(result)
}


func srem(s *Server, resp *Resp) error {
	if s.set == nil {
		s.set = newSet()
	}
	key := string(resp.Array[1].Value)
	member := string(resp.Array[2].Value)

	result := s.set.srem(key, member)
	return s.writeArgs(result)
}