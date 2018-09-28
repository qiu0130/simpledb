package simpledb

import (
	"testing"
	"log"
)

var client *Client

func init()  {
	client = NewClient()
}

func Print(resp *Resp) {
	if resp.Type == TypeArray {
		for _, a := range resp.Array {
			log.Printf("%+v", string(a.Value))
		}
	} else {
		log.Printf("%+v", string(resp.Value))
	}
}

func TestSet(t *testing.T) {

	resp, err := client.Set("foo", "bar")
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

}
