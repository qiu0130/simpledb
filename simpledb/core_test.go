package simpledb

import "testing"

func TestNewServer(t *testing.T) {
	server := NewServer()
	server.Run()
}

func TestGet(t *testing.T) {

	f := func(args ...interface{}) ([]interface{}, error) {
		t.Log("f len: ", len(args))
		t.Logf("f args: %v", args)

		return args, nil
	}

	g := func(args ...interface{}) {
		t.Log("g len: ", len(args))
		t.Logf("g args: %v", args)

	}

	res, _ := f("set", "foo", "bar")
	t.Logf("result: %v", res)
	g(res...)



	res, _ = f("set", []string{"foo", "bar"})
	t.Logf("result: %v", res)
	g(res...)

}