package simpledb

import (
	"container/list"
	"strconv"
	"sync"
)

// queue commands:
// lpush, rpush, lpop, rpop, lrem, lindex, llen, lrange, lset, ltrim, rpoplpush, llfush

type Queue struct {
	data map[string]*list.List
	mu   sync.RWMutex
}

func newQueue() *Queue {
	return &Queue{
		data: make(map[string]*list.List, defaultQueueSize),
		mu:   sync.RWMutex{},
	}
}

func (q *Queue) pushFront(key string, value interface{}) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.data[key]; ok {
		q.data[key].PushFront(value)
		return q.data[key].Len()
	}
	q.data[key] = list.New()
	q.data[key].PushFront(value)
	return q.data[key].Len()
}

func (q *Queue) pushBack(key string, value interface{}) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.data[key]; ok {
		q.data[key].PushBack(value)
		return q.data[key].Len()
	}
	q.data[key] = list.New()
	q.data[key].PushBack(value)
	return q.data[key].Len()
}

func (q *Queue) frontPop(key string) (interface{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.data[key]
	if !ok {
		return nil, empty
	}
	e := queue.Front()
	q.data[key].Remove(e)
	if e != nil {
		return e.Value, nil
	}
	return nil, empty
}

func (q *Queue) backPop(key string) (interface{}, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.data[key]
	if !ok {
		return nil, empty
	}
	e := queue.Back()
	q.data[key].Remove(e)
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
	queue, ok := q.data[key]
	if !ok {
		return empty
	}
	l := queue.Len()
	if index >= l {
		for e = queue.Front(); e != nil; {
			if e.Next() == nil {
				q.data[key].InsertAfter(value, e)
				return nil
			} else {
				e = e.Next()
			}
		}
	}
	for e = queue.Front(); e != nil; e = e.Next() {
		if index == i {
			q.data[key].InsertBefore(value, e)
			return nil
		}
		i += 1
	}
	return nil
}

func (q *Queue) Len(key string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.data[key]
	if !ok {
		return 0
	}
	return queue.Len()
}

func (q *Queue) remove(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.data, key)
	return nil
}

func (q *Queue) index(key string, index int) (interface{}, error) {

	q.mu.Lock()
	defer q.mu.Unlock()
	queue, ok := q.data[key]
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
	queue, ok := q.data[key]
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

func lLen(s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	l := s.queue.Len(key)
	return s.writeArgs(l)
}

func lPush(s *Server, resp *Resp) error {
	if s.queue == nil {
		s.queue = newQueue()
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	l := s.queue.pushFront(key, value)
	return s.writeArgs(l)
}

func lPop(s *Server, resp *Resp) error {

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

func rPush(s *Server, resp *Resp) error {

	if s.queue == nil {
		s.queue = newQueue()
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	l := s.queue.pushBack(key, value)
	return s.writeArgs(l)
}

func rPop(s *Server, resp *Resp) error {

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

func lRem(s *Server, resp *Resp) error {

	if s.queue == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	s.queue.remove(key)
	return s.reply1()

}

func lIndex(s *Server, resp *Resp) error {

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

func lSet(s *Server, resp *Resp) error {

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

func lRange(s *Server, resp *Resp) error {

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
