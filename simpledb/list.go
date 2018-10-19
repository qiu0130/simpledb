package simpledb

import (
	"container/list"
	"strconv"
	"sync"
)

/*
Queue commands:
	lpush, rpush, lpop, rpop, lrem, lindex, llen, lrange, lset, ltrim, rpoplpush, llfush
*/

type Queue struct {
	list map[string]*list.List
	mu   sync.RWMutex
}

func newQueue() *Queue {
	return &Queue{
		list: make(map[string]*list.List, defaultQueueSize),
		mu:   sync.RWMutex{},
	}
}

func (q *Queue) pushFront(key string, value interface{}) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.list[key]; ok {
		q.list[key].PushFront(value)
		return q.list[key].Len()
	}
	q.list[key] = list.New()
	q.list[key].PushFront(value)
	return q.list[key].Len()
}

func (q *Queue) pushBack(key string, value interface{}) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.list[key]; ok {
		q.list[key].PushBack(value)
		return q.list[key].Len()
	}
	q.list[key] = list.New()
	q.list[key].PushBack(value)
	return q.list[key].Len()
}

func (q *Queue) frontPop(key string) (interface{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return nil, empty
	}
	e := queue.Front()
	q.list[key].Remove(e)
	if e != nil {
		return e.Value, nil
	}
	return nil, empty
}

func (q *Queue) backPop(key string) (interface{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return nil, empty
	}
	e := queue.Back()
	q.list[key].Remove(e)
	if e != nil {
		return e.Value, nil
	}
	return nil, empty
}

func (q *Queue) set(key string, index int, value interface{}) error {
	var (
		i int
		e *list.Element
	)
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return empty
	}
	l := queue.Len()
	if index >= l {
		for e = queue.Front(); e != nil; {
			if e.Next() == nil {
				q.list[key].InsertAfter(value, e)
				return nil
			} else {
				e = e.Next()
			}
		}
	}
	for e = queue.Front(); e != nil; e = e.Next() {
		if index == i {
			q.list[key].InsertBefore(value, e)
			return nil
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

func (q *Queue) index(key string, index int) (interface{}, error) {

	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return nil, empty
	}
	var i int
	for e := queue.Front(); e != nil; e = e.Next() {
		if index == i {
			return e.Value, nil
		}
		i += 1
	}
	return nil, empty
}

func (q *Queue) ranges(key string, start, stop int) ([]string, error) {

	var (
		i int
		s []string
	)
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.list[key]
	if !ok {
		return nil, empty
	}
	for e := queue.Front(); e != nil; e = e.Next() {
		if start <= i && i < stop {
			s = append(s, e.Value.(string))
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

	l := s.queue.pushFront(key, value)
	return s.writeArgs(l)
}

func lpop(s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	val, err := s.queue.frontPop(key)
	if err != nil {
		return s.replyErr(err)
	}
	return s.writeArgs(val)
}

func rpush(s *Server, resp *Resp) error {

	if s.queue == nil {
		s.queue = newQueue()
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	l := s.queue.pushBack(key, value)
	return s.writeArgs(l)
}

func rpop(s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	val, err := s.queue.backPop(key)
	if err != nil {
		return s.replyErr(err)
	}
	return s.writeArgs(val)
}

func lrem(s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	s.queue.remove(key)
	return s.reply1()

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
	value := string(resp.Array[3].Value)
	err = s.queue.set(key, index, value)
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
