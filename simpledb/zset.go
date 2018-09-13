package simpledb

// Zset Command

type Member struct {
	member string
	score float64
}

type SortedSet struct {
	key map[string][]*Member
}

func newSortedSet() *SortedSet {
	return &SortedSet{key: make(map[string][]*Member, defaultSortedSetSize)}
}

func (s *SortedSet) zadd(key string, socre float64, member string) {

}
