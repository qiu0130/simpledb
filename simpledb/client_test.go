package simpledb

import (
	"fmt"
	"testing"
)

var client *Client

func init() {
	client = NewClient()
}

func Print(resp *Resp, err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	} else {
		if resp.Type == TypeArray {
			for _, a := range resp.Array {
				t.Logf("%+v", string(a.Value))
			}
		} else {
			t.Logf("%+v", string(resp.Value))

		}
	}
}

// str test

func TestClient_Set(t *testing.T) {
	resp, err := client.Set("foo", "bar")
	Print(resp, err, t)
	resp, err = client.Set("integer", "100")
	Print(resp, err, t)
	resp, err = client.Set("test0", "test1")
	Print(resp, err, t)
	resp, err = client.Set("test1", "test3")
	Print(resp, err, t)
}

func TestClient_Get(t *testing.T) {
	resp, err := client.Get("foo")
	Print(resp, err, t)
	resp, err = client.Get("integer")
	Print(resp, err, t)
	resp, err = client.Get("test0")
	Print(resp, err, t)
	resp, err = client.Get("test1")
	Print(resp, err, t)
}

func TestClient_Incr(t *testing.T) {
	resp, err := client.Incr("integer")
	Print(resp, err, t)
}

func TestClient_IncrBy(t *testing.T) {
	resp, err := client.IncrBy("integer", 100)
	Print(resp, err, t)
}

func TestClient_Decr(t *testing.T) {
	resp, err := client.Decr("integer")
	Print(resp, err, t)
}

func TestClient_DecrBy(t *testing.T) {
	resp, err := client.DecrBy("integer", 100)
	Print(resp, err, t)
}

func TestClient_Append(t *testing.T) {
	resp, err := client.Append("foo", "bar")
	Print(resp, err, t)
}

func TestClient_Delete(t *testing.T) {
	resp, err := client.Delete("test1", "test2")
	Print(resp, err, t)
}

func TestClient_MGet(t *testing.T) {
	resp, err := client.MGet("foo", "test1")
	Print(resp, err, t)
}

func TestClient_MSet(t *testing.T) {

	mock := make(map[string]interface{})
	mock["foo"] = "1"
	mock["test1"] = 1000

	resp, err := client.MSet(mock)
	Print(resp, err, t)
}

// list test

func TestClient_pushFront(t *testing.T) {

}

func TestDefaultClient(t *testing.T) {
	var a interface{}
	d := make(map[string]interface{})
	d["test"] = 1
	d["test1"] = 1

	a = d

	switch t := a.(type) {
	case map[string]interface{}:

		for k, v := range t {
			fmt.Println(k, v)
		}
		fmt.Printf("0, %T, %v", t, t)
		fmt.Println(len(t))
	default:
		fmt.Printf("1, %T, %v", t, t)

	}
}
