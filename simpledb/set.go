package simpledb

/*
Set commands:
sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismenber, smembers, spop, srem, sunion, sunionstore
*/

type Set struct {
	value map[string]bool
	len   int
}

func (s *Set) add(member string) error {

	s.value[member] = true
	s.len++
	return nil
}
