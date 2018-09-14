package simpledb

import (
	"container/list"
	"sync"
	"strconv"
)

/*
Queue commands:
	lpush, rpush, lpop, rpop, lrem, lindex, llen, lrange, lset, ltrim, rpoplpush, llfush
 */


type Queue struct {
	list map[string]*list.List
	mu sync.RWMutex
}

func newQueue() *Queue {
	return &Queue{
		list: make(map[string]*list.List, defaultQueueSize),
		mu: sync.RWMutex{},
	}
}

func (q *Queue) leftPush(key string, value interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return empty
	}
	queue.PushFront(value)
	return nil
}

func (q *Queue) rightPush(key string, value interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return empty
	}
	queue.PushBack(value)
	return nil
}


func (q *Queue) leftPop(key string) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return "", empty
	}
	ele := queue.Front()
	if ele != nil {
		v, ok := ele.Value.(string)
		if !ok {
			return "", errStr
		}
		return v, nil
	}
	return "", empty
}


func (q *Queue) rightPop(key string) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return "", empty
	}
	ele := queue.Back()
	if ele != nil {
		v, ok := ele.Value.(string)
		if !ok {
			return "", errStr
		}
		return v, nil
	}
	return "", empty
}


func (q *Queue) leftSet(key string, index int, value string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return empty
	}
	var i int
	for e := queue.Front(); e != nil; e.Next() {
		if index == i {
			queue.InsertAfter(value, e)
		}
		i += 1
	}
	return nil
}

func (q *Queue) Len(key string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return 0
	}
	return queue.Len()
}

func (q *Queue) remove(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.list, key)
	return nil
}

func (q *Queue) index(key string, index int) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return "", empty
	}
	var i int
	for e := queue.Front(); e != nil; e.Next() {
		if index == i {
			ele := e.Value
			if ele != nil {
				return "", empty
			}
			v, ok := ele.(string)
			if !ok {
				return "", errStr
			}
			return v, nil
		}
		i += 1
	}
	return "", empty
}

func (q *Queue) ranges(key string, start, stop int) ([]string, error) {

	var s []string
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return nil, empty
	}
	var i int
	for e := queue.Front(); e != nil; e.Next() {
		if start == i && stop <= i{
			ele := e.Value
			if ele != nil {
				return nil, empty
			}
			v, ok := ele.(string)
			if !ok {
				return nil, errStr
			}
			s = append(s, v)
		}
		i += 1
	}
	return s, nil
}


func llen(s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	l := s.queue.Len(key)
	return s.writeArgs(l)
}


func lpush(s *Server, resp *Resp) error {
	if s.queue == nil {
		s.queue = newQueue()
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	err := s.queue.leftPush(key, value)
	if err != nil {
		return s.reply0()
	}
	return s.reply1()
}

func lpop (s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	v, err := s.queue.leftPop(key)
	if err != nil {
		if err == empty {
			return s.replyNil()
		}
		return s.replyErr(err)
	}
	return s.writeArgs(v)
}


func rpush(s *Server, resp *Resp) error {
	if s.queue == nil {
		s.queue = newQueue()
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	err := s.queue.rightPush(key, value)
	if err != nil {
		return s.reply0()
	}
	return s.reply1()
}

func rpop (s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	v, err := s.queue.rightPop(key)
	if err != nil {
		if err == empty {
			return s.replyNil()
		}
		return s.replyErr(err)
	}
	return s.writeArgs(v)
}


func lrem(s *Server, resp *Resp) error {
	if s.queue == nil {
		return s.reply0()
	}

}


func lindex(s *Server, resp *Resp) error {
	if s.queue == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	index, err := strconv.Atoi(string(resp.Array[2].Value))
	if err != nil {
		return s.replyErr(errInteger)
	}
	v, err := s.queue.index(key, index)
	if err != nil {
		return s.reply0()
	}
	return s.writeArgs(v)
}


func lset(s *Server, resp *Resp) error {
	if s.queue == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	index, err := strconv.Atoi(string(resp.Array[2].Value))
	if err != nil {
		return s.replyErr(errInteger)
	}
	value := string(resp.Array[2].Value)

	err = s.queue.leftSet(key, index, value)
	if err != nil {
		return s.reply0()
	}
	return s.reply1()
}


func lrange(s *Server, resp *Resp) error {
	if s.queue == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	start, err := strconv.Atoi(string(resp.Array[2].Value))
	if err != nil {
		return s.replyErr(errInteger)
	}
	stop, err := strconv.Atoi(string(resp.Array[3].Value))
	if err != nil {
		return s.replyErr(errInteger)
	}

	v, err := s.queue.ranges(key, start, stop)
	if err != nil {
		return s.replyNil()
	}
	return s.writeArgs(v)
}


