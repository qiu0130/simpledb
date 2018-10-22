package simpledb

import (
	"strconv"
	"sync"
)

// str commands:
// append, decr, decrby, incr, incrby, mdelete, mget, mset, get, set, del, exists, len, flush

// setnx
// setex
// msetex

type Dict struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

func newDict() *Dict {
	return &Dict{
		mu:   sync.RWMutex{},
		data: make(map[string]interface{}, defaultDictSize),
	}
}

func (d *Dict) delete(k string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.data, k)
	return nil
}

func (d *Dict) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.data)
}

func (d *Dict) add(k string, args interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data[k] = args
}

func (d *Dict) get(k string) (interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if v, ok := d.data[k]; ok {
		return v, nil
	}
	return nil, empty
}

func (d *Dict) getInt64(k string) (int64, error) {
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
		v   int64
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
		v   int64
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
		v   int64
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
		v   int64
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
	for _, args := range resp.Array[1:] {
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
	var res []string
	for i, args := range resp.Array[1:] {
		val, err := s.dict.get(string(args.Value))
		index := strconv.Itoa(i) + ") "
		if err != nil {
			res = append(res, index+"nil")
		} else {
			v, ok := val.(string)
			if ok {
				res = append(res, index+v)
			} else {
				res = append(res, index+"nil")
			}
		}
	}
	if len(res) > 0 {
		return s.writeArgs(res[0], res[1:])
	}
	return s.writeArgs("nil")
}
