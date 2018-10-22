package simpledb

import "testing"

var (
	s *Set
)

func init() {
	s = newSet()
}

func TestSet_Add(t *testing.T) {

	s.add("foo", "t1", "t2")
	t.Log(s.card("foo"))

	s.add("foo", "t1", "t2", "t1", "t2")
	t.Log(s.card("foo"))
}

func TestSet_Diff(t *testing.T) {

	s.add("foo", "t1", "t2")
	s.add("bar", "t3", "t1")
	t.Log(s.diff("foo", "bar"))
}

func TestSet_Inter(t *testing.T) {

	s.add("foo", "t1", "t2")
	s.add("bar", "t", "t1")

	t.Log(s.inter("foo", "bar"))

}

func TestSet_Union(t *testing.T) {

	s.add("foo", "t1", "t2")
	s.add("bar", "t3", "t4")
	t.Log(s.union("foo", "bar"))

}

func TestSet_IsMember(t *testing.T) {

	s.add("foo", "t1", "t2")
	t.Log(s.sIsMember("foo", "t1"))
	t.Log(s.sIsMember("foo", "t"))
}

func TestSet_sMembers(t *testing.T) {

	s.add("foo", "t1", "t2")
	t.Log(s.sMembers("foo"))
}

func TestSet_sRem(t *testing.T) {

	s.add("foo", "t1", "t2")
	t.Log(s.sRem("foo", "t1"))
	t.Log(s.sMembers("foo"))
}
