package simpledb

/*

Hash commands:
	hel, hexists, hget, hincrby, hkeys, hlen, hmget, hsmet, hset, hsetnx, hvals

 */
type Hash struct {
	key string
	filed map[string]string
}

func newHash() []Hash {
	return make([]Hash, defaultHashSize)
}

func getAll(hash []Hash, key string) (map[string]string, error) {
	for _, h := range hash {
		if h.key == key {
			return h.filed, nil
		}
	}
	return nil, empty
}

func set(hash []Hash, key, field, value string) error {
	h := Hash{key: key, }
	hash = append(hash, )
}

func hdel(hash []Hash, key string, field []string) (err error) {

	fields, err := getAll(hash, key)
	if err != nil {
		return
	}
	for _, key := range field {
		delete(fields, key)
	}
	return nil
}

func hDel(s *Server, resp *Resp) error {

	if s.hash == nil {
		return s.reply0()
	}
	var filed []string
	key := string(resp.Array[1].Value)
	for _, f := range resp.Array[1:] {
		filed = append(filed, string(f.Value))
	}
	return hdel(s.hash, key, filed)
}

func hExists(s *Server, resp *Resp) error {

	if s.hash == nil {
		return s.reply0()
	}
	key := string(resp.Array[1].Value)
	value := string(resp.Array[2].Value)
	fields, err := getAll(s.hash, key)
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
	fields, err := getAll(s.hash, key)
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
	s.hash = append(s.hash, Hash{key:key, filed: h})
	return s.reply1()
}

func hGetAll(s *Server, resp *Resp) error {
	var args []string
	if s.hash == nil {
		return s.replyNil()
	}
	key := string(resp.Array[1].Value)
	fields, err := getAll(s.hash, key)
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
	fields, err := getAll(s.hash, key)
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
	fields, err := getAll(s.hash, key)
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
	fields, err := getAll(s.hash, key)
	if err != nil {
		return s.reply0()
	}
	return s.writeArgs(len(fields))
}
func hMget(s *Server, resp *Resp) error {

}

func hMSet(s *Server, resp *Resp) error {

}
