package simpledb

import (
	"testing"
	"log"
)

var client *Client

<<<<<<< HEAD
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
=======
func init()  {
	client = NewClient()
}

func Print(resp *Resp) {
	if resp.Type == TypeArray {
		for _, a := range resp.Array {
			log.Printf("%+v", string(a.Value))
>>>>>>> a4428d3773f0b205aa5251b470fd424c4eafbc9e
		}
	} else {
		log.Printf("%+v", string(resp.Value))
	}
}

<<<<<<< HEAD
func PrintValue(key string, t *testing.T) {
	resp, err := client.Get(key)
	Print(resp, err, t)
}
=======
func TestSet(t *testing.T) {
>>>>>>> a4428d3773f0b205aa5251b470fd424c4eafbc9e

func TestClientDict(t *testing.T) {
	resp, err := client.Set("foo", "bar")
<<<<<<< HEAD
	Print(resp, err, t)
	resp, err = client.Set("integer", "100")
	Print(resp, err, t)
	resp, err = client.Set("test0", "test1")
	Print(resp, err, t)
	resp, err = client.Set("test2", "test3")
	Print(resp, err, t)

	PrintValue("foo", t)
	PrintValue("integer", t)
	PrintValue("test0", t)
	PrintValue("test2", t)

	resp, err = client.Incr("integer")
	Print(resp, err, t)

	resp, err = client.IncrBy("integer", 100)
	Print(resp, err, t)

	resp, err = client.Decr("integer")
	Print(resp, err, t)

	resp, err = client.DecrBy("integer", 99)
	Print(resp, err, t)
=======
	if err != nil {
		t.Fatal(err)
	}
	Print(resp)
}

func TestGet(t *testing.T) {

	resp, err := client.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	Print(resp)

>>>>>>> a4428d3773f0b205aa5251b470fd424c4eafbc9e
}

//func TestSet(t *testing.T) {
//	resp, err := client.Set("foo", "bar")
//	Print(resp, err, t)
//}
//
//func TestGet(t *testing.T) {
//	resp, err := client.Get("foo")
//	Print(resp, err, t)
//}


