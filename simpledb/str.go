package simpledb

import (
	"sync"
	"strconv"
	"log"
)

/*
K/V commands:
	append
	decr
	decrby
	incr
	incrby
    mdelete
	mget
    mset
	get
	set
	del
	exists
	//setnx
	//setex
	//msetex
	len
	flush
 */

const (
	decr = iota
	decrBy
	incr
	incrBy
)

type Dict struct {
	mu    sync.RWMutex
	value map[string]interface{}
}

func newDict() *Dict {
	return &Dict{
		mu:    sync.RWMutex{},
		value: make(map[string]interface{}, defaultDictSize),
	}
}

func (d *Dict) delete(k string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.value, k)
	return nil
}

func (d *Dict) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.value)
}

func (d *Dict) add(k string, args interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.value[k] = args
}

func (d *Dict) get(k string) (interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if v, ok := d.value[k]; ok {
		return v, nil
	}
	return "", empty
}

func (d *Dict) getInt64(k string, v int64) (int64,  error) {
	value, err := d.get(k)
	if err != nil {
		return v, nil
	}
	log.Println(value, err)
	v, ok := value.(int64)
	if !ok {
		return 0, errInteger
	}
	return v, nil
}


func set(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)
	s.dict.add(key, value)

	return s.replyOk()
}

func get(s *Server, resp *Resp) error {
	if s.dict == nil {
		return s.replyNil()
	}

	key := string(resp.Array[1].Value)
	value, err := s.dict.get(key)
	if err != nil {
		return s.replyNil()
	}
	strValue, ok := value.(string)
	if !ok {
		return s.replyErr(errStr)
	}

	return s.writeArgs(strValue)
}

func op(s *Server, resp *Resp, op int) (value int64, err error) {
	var vv, v int64
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	switch op {
	case decr:
		v, err = s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = v-1

	case decrBy:
		v, err = strconv.ParseInt(string(resp.Array[2].Value), 10, 0)
		if err != nil {
			return
		}
		vv, err = s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = vv - v
	case incr:
		v, err = s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = v+1
	case incrBy:
		v, err = strconv.ParseInt(string(resp.Array[2].Value), 10, 0)
		if err != nil {
			return
		}
		vv, err = s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = vv+v
	}
	s.dict.add(key, value)
	return value, nil
}

func decrease(s *Server, resp *Resp) error {
	v, err := op(s, resp, decr)
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)
}

func decreaseBy(s *Server, resp *Resp) error {
	v, err := op(s, resp, decrBy)
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)
}

func increase(s *Server, resp *Resp) error {
	v, err := op(s, resp, incr)
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)
}

func increaseBy(s *Server, resp *Resp) error {
	v, err := op(s, resp, incrBy)
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)

}

func appends(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	v, err := s.dict.get(key)
	if err != nil {
		vv, ok := v.(string)
		if !ok {
			return s.replyErr(errStr)
		}
		newValue := vv + value
		s.dict.add(key, newValue)
		return s.writeArgs(newValue)
	}
	s.dict.add(key, value)
	return s.writeArgs(value)
}

func del(s *Server, resp *Resp) error {

	key := string(resp.Array[1].Value)
	_, err := s.dict.get(key)
	if err != nil {
		return s.reply0()
	}
	s.dict.delete(key)
	return s.reply1()
}

func exists(s *Server, resp *Resp) error {
	key := string(resp.Array[1].Value)
	_, err := s.dict.get(key)
	if err != nil {
		return s.reply0()
	}
	return s.reply1()
}

func mSet(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	l := len(resp.Array)
	// todo len can't enough
	for i := 1; i < l; i += 2 {
		key := string(resp.Array[1].Value)
		value := string(resp.Array[2].Value)
		s.dict.add(key, value)
	}
	return s.replyOk()
}

func mGet(s *Server, resp *Resp) error {

	for _, args := range resp.Array[1:] {
		v, _ := s.dict.get(string(args.Value))
		_, err := s.wb.WriteArgs(v)
		if err != nil {
			return s.replyErr(err)
		}
	}
	return s.replyOk()
}

func mDelete(s *Server, resp *Resp) error {
	for _, args := range resp.Array[1:] {
		err := s.dict.delete(string(args.Value))
		if err != nil {
			return s.replyErr(err)
		}
	}
	return s.replyOk()
}