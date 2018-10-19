package simpledb

import (
	"testing"
)

var (
	q *Queue
)

func init() {
	q = newQueue()
}

func TestPushFront(t *testing.T) {
	t.Log(q.pushFront("foo", 1))
	t.Log(q.pushFront("foo", 2))
	t.Log(q.pushFront("foo", 3))
	t.Log(q.pushFront("foo", 4))

	for i := 0; i < 4; i++ {
		t.Log(q.frontPop("foo"))
	}
}

func TestPushBack(t *testing.T) {
	t.Log(q.pushBack("foo", 1))
	t.Log(q.pushBack("foo", 2))
	t.Log(q.pushBack("foo", 3))
	t.Log(q.pushBack("foo", 4))

	for i := 0; i < 4; i++ {
		t.Log(q.backPop("foo"))
	}
}

func TestSet(t *testing.T) {
	// 2 1 10
	q.pushFront("foo", 1)
	q.pushFront("foo", 2)
	t.Log(q.set("foo", 10, 10))

	for i := 0; i < 3; i++ {
		t.Log(q.frontPop("foo"))

	}
}

func TestRemove(t *testing.T) {

	q.pushFront("foo", 1)
	t.Log(q.Len("foo"))
	q.remove("foo")
	t.Log(q.Len("foo"))
}

func TestIndex(t *testing.T) {
	q.pushFront("foo", 1)
	q.pushFront("foo", 2)
	q.pushFront("foo", 3)
	q.pushFront("foo", 4)
	q.pushFront("foo", 5)

	t.Log(q.index("foo", 0))
	t.Log(q.index("foo", 1))
	t.Log(q.index("foo", 2))
	t.Log(q.index("foo", 3))
	t.Log(q.index("foo", 4))

}

func TestRanges(t *testing.T) {
	q.pushFront("foo", 1)
	q.pushFront("foo", 2)
	q.pushFront("foo", 3)
	q.pushFront("foo", 4)
	q.pushFront("foo", 5)

	t.Log(q.ranges("foo", 0, 2))
	t.Log(q.ranges("foo", 0, 4))
	t.Log(q.ranges("foo", 2, 3))

}
