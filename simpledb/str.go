package simpledb

import (
	"sync"
	"strconv"
	"fmt"
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
	return nil, empty
}

func (d *Dict) getInt64(k string) (int64,  error) {
	val, err := d.get(k)
	if err != nil {
		return 0, nil
	}
	if v, ok := val.(string); ok {
		t, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errInteger
		}
		return t, nil
	}
	return 0, errInteger
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


func decrease(s *Server, resp *Resp) error {
	var (
		v int64
		err error
	)
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	v, err = s.dict.getInt64(key)
	if err != nil {
		return s.replyErr(err)
	}
	v = v - 1
	s.dict.add(key, strconv.FormatInt(v, 10))
	return s.writeArgs(v)
}

func decreaseBy(s *Server, resp *Resp) error {
	var (
		v int64
		err error
	)
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	val, err := strconv.ParseInt(string(resp.Array[2].Value), 10, 64)
	if err != nil {
		return s.replyErr(err)
	}
	v, err = s.dict.getInt64(key)
	if err != nil {
		return s.replyErr(err)
	}
	v = v - val
	s.dict.add(key, strconv.FormatInt(v, 10))
	return s.writeArgs(v)
}

func increase(s *Server, resp *Resp) error {
	var (
		v int64
		err error
	)
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	v, err = s.dict.getInt64(key)
	if err != nil {
		return s.replyErr(err)
	}
	v = v + 1
	s.dict.add(key, strconv.FormatInt(v, 10))
	return s.writeArgs(v)
}

func increaseBy(s *Server, resp *Resp) error {
	var (
		v int64
		err error
	)
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	val, err := strconv.ParseInt(string(resp.Array[2].Value), 10, 64)
	if err != nil {
		return s.replyErr(err)
	}
	v, err = s.dict.getInt64(key)
	if err != nil {
		return s.replyErr(err)
	}
	v = v + val
	s.dict.add(key, strconv.FormatInt(v, 10))
	return s.writeArgs(v)

}

func appends(s *Server, resp *Resp) error {

	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)

	val, err := s.dict.get(key)
	if err != nil {
		s.dict.add(key, value)
		return s.writeArgs(len(value))
	}
	if v, ok := val.(string); ok {
		newValue := v + value
		s.dict.add(key, newValue)
		return s.writeArgs(len(newValue))
	}
	return s.replyErr(errStr)
}

func deletes(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	for _, args := range resp.Array {
		fmt.Println(string(args.Value))
	}

	for _, args := range resp.Array[1:] {
		fmt.Println(string(args.Value))
		err := s.dict.delete(string(args.Value))
		if err != nil {
			return s.replyErr(err)
		}
	}
	return s.replyOk()
}

func exists(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	key := string(resp.Array[1].Value)
	_, err := s.dict.get(key)
	if err != nil {
		return s.reply0()
	}
	return s.reply1()
}

func multipleSet(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	l := len(resp.Array)
	// todo len can't enough
	for i := 1; i < l; i += 2 {
		key := string(resp.Array[i].Value)
		value := string(resp.Array[i+1].Value)
		s.dict.add(key, value)
	}
	return s.replyOk()
}

func multipleGet(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	for _, args := range resp.Array[1:] {
		v, err := s.dict.get(string(args.Value))
		if err != nil {
			s.wb.WriteArgs(nil)
		}
		_, err = s.wb.WriteArgs(v)
		if err != nil {
			return s.replyErr(err)
		}
	}
	return s.replyOk()
}

