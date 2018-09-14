package simpledb

import (
	"testing"
	"container/list"
	"fmt"
)

func TestQueue_Len(t *testing.T) {
	l := list.New()
	e4 := l.PushBack(4)
	e1 := l.PushFront(1)

	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
	l.InsertBefore("xx", e4)
	l.InsertAfter("yy", e1)

	// Iterate through list and print its contents.
	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}