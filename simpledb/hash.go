package simpledb

// hash commands:
// hel, hexists, hget, hincrby, hkeys, hlen, hmget, hsmet, hset, hsetnx, hvals

type Hash struct {
	key   string
	filed map[string]string
}

func newHash() []*Hash {
	return make([]*Hash, defaultHashSize)
}

func getFiled(hash []*Hash, key string) (map[string]string, error) {
	for _, h := range hash {
		if h.key == key {
			return h.filed, nil
		}
	}
	return nil, empty
}

func hDel(s *Server, resp *Resp) error {
	if s.hash == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	for _, f := range resp.Array[1:] {
		filed, err := getFiled(s.hash, key)
		if err != nil {
			s.replyErr(err)
		}
		delete(filed, string(f.Value))
	}
	return s.reply1()
}

func hExists(s *Server, resp *Resp) error {

	if s.hash == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)
	fields, err := getFiled(s.hash, key)
	if err != nil {
		return s.reply0()
	}
	if _, ok := fields[value]; ok {
		return s.reply1()
	}
	return s.reply0()
}

func hGet(s *Server, resp *Resp) error {
	if s.hash == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)
	fields, err := getFiled(s.hash, key)
	if err != nil {
		return s.replyNil()
	}
	if v, ok := fields[value]; ok {
		return s.writeArgs(v)
	}
	return s.replyNil()
}

func hSet(s *Server, resp *Resp) error {
	if s.hash == nil {
		s.hash = newHash()
	}
	key := string(resp.Array[1].Value)
	field := string(resp.Array[2].Value)
	value := string(resp.Array[3].Value)

	h := make(map[string]string)
	h[field] = value
	s.hash = append(s.hash, &Hash{key: key, filed: h})
	return s.reply1()
}

func hGetAll(s *Server, resp *Resp) error {
	var args []string
	if s.hash == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	fields, err := getFiled(s.hash, key)
	if err != nil {
		return s.replyNil()
	}
	for k, v := range fields {
		args = append(args, k)
		args = append(args, v)
	}
	return s.writeArgs(args)

}

func hKeys(s *Server, resp *Resp) error {
	var args []string
	if s.hash == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	fields, err := getFiled(s.hash, key)
	if err != nil {
		return s.reply0()
	}

	for k, _ := range fields {
		args = append(args, k)
	}
	return s.writeArgs(args)
}

func hVals(s *Server, resp *Resp) error {
	var args []string
	if s.hash == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	fields, err := getFiled(s.hash, key)
	if err != nil {
		return s.reply0()
	}

	for v := range fields {
		args = append(args, v)
	}
	return s.writeArgs(args)
}

func hLen(s *Server, resp *Resp) error {
	if s.hash == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	fields, err := getFiled(s.hash, key)
	if err != nil {
		return s.reply0()
	}
	return s.writeArgs(len(fields))
}

func hMGet(s *Server, resp *Resp) error {
	if s.hash == nil {
		return s.replyNil()
	}
	var reply []string
	key := string(resp.Array[1].Value)
	// typo Array
	for _, filed := range resp.Array[1].Array {
		fields, err := getFiled(s.hash, key)
		if err != nil {
			return s.replyNil()
		}
		if v, ok := fields[string(filed.Value)]; ok {
			reply = append(reply, v)
		}
	}
	if len(reply) > 0 {
		return s.writeArgs(reply)
	}
	return s.replyNil()

}

func hMSet(s *Server, resp *Resp) error {
	if s.hash == nil {
		s.hash = newHash()
	}
	key := string(resp.Array[1].Value)

	store := func(hash map[string]string) {
		l := len(resp.Array[2].Array)
		array := resp.Array[2].Array
		for i := 0; i < l; i += 2 {
			field := string(array[i].Value)
			value := string(array[i+1].Value)
			hash[field] = value
		}
	}

	for _, hash := range s.hash {
		if hash.key == key {
			store(hash.filed)
			return s.reply1()
		}
	}
	// new element
	h := &Hash{key: key, filed: make(map[string]string, defaultHashSize)}
	store(h.filed)
	s.hash = append(s.hash, h)

	return s.replyNil()
}
