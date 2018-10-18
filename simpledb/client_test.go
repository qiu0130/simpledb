package simpledb

import (
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
	resp, err := client.Delete([]string{"foo", "test0"})
	Print(resp, err, t)
}

func TestClient_MGet(t *testing.T) {
	resp, err := client.MGet([]string{"foo", "test0"})
	Print(resp, err, t)
}

func TestClient_MSet(t *testing.T) {

	mock := make(map[string]interface{})
	mock["foo1"] = "1"
	mock["test1"] = "test2"

	resp, err := client.MSet(mock)
	Print(resp, err, t)
}

