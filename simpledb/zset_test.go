package simpledb

import "testing"

var (
	z *SortedSet
)

func init() {
	z = newSortedSet()
}

func TestSortSet_zAdd(t *testing.T) {

	t.Log(z.zAdd("foo", 1, "t1"))
	t.Log(z.zAdd("foo", 2, "t2"))
	t.Log(z.zAdd("foo", 3, "t3"))

}

func TestSortSet_zCard(t *testing.T) {

	t.Log(z.zCard("foo"))
	z.zAdd("foo", 1, "t1")
	z.zAdd("foo", 2, "t2")
	t.Log(z.zCard("foo"))

}

func TestSortSet_zCount(t *testing.T) {
	z.zAdd("foo", 1, "t1")
	z.zAdd("foo", 10, "t2")
	z.zAdd("foo", 20, "t3")
	z.zAdd("foo", 30, "t4")

	t.Log(z.zCount("foo", 1, 11))
}

func TestSortSet_zIncrementBy(t *testing.T) {
	z.zAdd("foo", 100, "t")
	t.Log(z.zIncrementBy("foo", 100, "t"))
	t.Log(z.zRange("foo", 0, 10, true))
}

func TestSortSet_zRange(t *testing.T) {
	z.zAdd("foo", 1, "t1")
	z.zAdd("foo", 10, "t2")
	z.zAdd("foo", 20, "t3")
	z.zAdd("foo", 30, "t4")

	t.Log(z.zRange("foo", 1, -1, true))
}

func TestSortSet_zRangeByScore(t *testing.T) {
	z.zAdd("foo", 1, "t1")
	z.zAdd("foo", 10, "t2")
	z.zAdd("foo", 20, "t3")
	z.zAdd("foo", 30, "t4")

	t.Log(z.zRangeByScore("foo", 10, 20, true))
}

func TestSortSet_zRank(t *testing.T) {
	z.zAdd("foo", 1, "t1")
	z.zAdd("foo", 10, "t2")
	z.zAdd("foo", 20, "t3")
	z.zAdd("foo", 30, "t4")

	t.Log(z.zRank("foo", "t2"))
}

func TestSortSet_zRem(t *testing.T) {
	z.zAdd("foo", 1, "t1")
	z.zAdd("foo", 10, "t2")
	z.zAdd("foo", 20, "t3")
	z.zAdd("foo", 30, "t4")

	t.Log(z.zRem("foo", "t2"))

	t.Log(z.zRange("foo", 0, -1, true))
}
