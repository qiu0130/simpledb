package simpledb

import "container/list"

/*
Queue commands:
	lpush, rpush, lpop, rpop, lrem, lindex, llen, lrange, lset, ltrim, rpoplpush, llfush
 */


type Queue struct {
	list *list.List
	len int
}

func newQueue() *Queue {
	return &Queue{
		list: list.New(),
		len: 0,
	}
}

func (q *Queue) lpush(v string) {
	q.list.PushFront(v)
}

func (q *Queue) rpush(v string) {
	q.list.PushBack(v)
}

func (q *Queue) size() int {
	return q.list.Len()
}

func (q *Queue) lpop() string {
	ele := q.list.Front()
	if ele != nil {
		v, ok := ele.Value.(string)
		if ok {
			return v
		}
	}
	return ""
}
