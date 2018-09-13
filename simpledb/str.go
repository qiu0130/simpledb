package simpledb

import (
	"sync"
	"strconv"
)

/*
K/V commands:
	append, decr, decrby,  incr, incrby, mdelete, mget, mset, mpop,
	get, set, delete, exists, get, getset,
	setnx, setex, msetex, len, flush
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

func (d *Dict) set(k string, args interface{}) {
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
	return nil, kvErr
}

func (d *Dict) getInt64(k string, v int64) (int64,  error) {

	value, err := d.get(k)
	if err != nil {
		return v, nil
	}
	v, ok := value.(int64)
	if !ok {
		return 0, invalidInteger
	}
	return v, nil
}


func set(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = &Dict{
			mu:    sync.RWMutex{},
			value: make(map[string]interface{}),
		}
	}

	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)
	s.dict.set(key, value)

	return s.replyOk()
}

func get(s *Server, resp *Resp) error {
	if s.dict == nil {
		return s.writeArgs(kvErr)
	}

	key := string(resp.Array[1].Value)
	value, err := s.dict.get(key)
	if err != nil {
		return s.writeArgs(err)
	}
	strValue, _ := value.(string)

	return s.writeArgs(strValue)
}

func op(s *Server, resp *Resp, op string) (value int64, err error) {
	if s.dict == nil {
		s.dict = newDict()
	}

	key := string(resp.Array[1].Value)
	switch op {
	case "-":
		v, err := s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = v-1

	case "-!":
		v, err := strconv.ParseInt(string(resp.Array[2].Value), 10, 0)
		if err != nil {
			return
		}
		vv, err := s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = vv - v
	case "+":
		v, err := s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = v+1
	case "+!":
		v, err := strconv.ParseInt(string(resp.Array[2].Value), 10, 0)
		if err != nil {
			return
		}
		vv, err := s.dict.getInt64(key, 0)
		if err != nil {
			return
		}
		value = vv+v
	}
	s.dict.set(key, value)
	return value, nil
}

func decrease(s *Server, resp *Resp) error {
	v, err := op(s, resp, "-")
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)

}

func decreaseBy(s *Server, resp *Resp) error {
	v, err := op(s, resp, "-!")
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)
}

func increase(s *Server, resp *Resp) error {
	v, err := op(s, resp, "+")
	if err != nil {
		return s.writeArgs(err)
	}
	return s.writeArgs(v)

}

func increaseBy(s *Server, resp *Resp) error {
	v, err := op(s, resp, "+!")
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
			return s.writeArgs(invalidString)
		}
		newValue := vv + value
		s.dict.set(key, newValue)
		return s.writeArgs(newValue)
	}
	s.dict.set(key, value)
	return s.writeArgs(value)
}

func mSet(s *Server, resp *Resp) error {
	if s.dict == nil {
		s.dict = newDict()
	}
	l := len(resp.Array)
	for i := 1; i < l; i+=2 {
		key := string(resp.Array[1].Value)
		value := string(resp.Array[2].Value)
		s.dict.set(key, value)
	}
	return s.replyOk()
}

func mGet(s *Server, resp *Resp) error {

	for _, args := range resp.Array[1:] {
		v, _ := s.dict.get(string(args.Value))
		_, err := s.wb.WriteArgs(v)
		if err != nil {
			return err
		}
	}
	return s.flush()
}

func del(s *Server, resp *Resp) error {

	key := string(resp.Array[1].Value)
	_, err := s.dict.get(key)
	if err != nil {
		return s.replyNull()
	}
	s.dict.delete(key)
	return s.replyOk()
}

func exists(s *Server, resp *Resp) error {
	key := string(resp.Array[1].Value)
	_, err := s.dict.get(key)
	if err != nil {
		return s.replyNull()
	}
	return s.replyOk()
}
