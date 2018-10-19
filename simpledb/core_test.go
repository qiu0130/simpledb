package simpledb

import "testing"

func TestNewServer(t *testing.T) {
	server := NewServer()
	server.Run()
}
