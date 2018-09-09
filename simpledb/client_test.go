package simpledb

import "testing"

func TestNewClient(t *testing.T) {
	client := NewClient()

	resp, err := client.Set("foo", "bar")
	if err != nil {
		t.Error(err)
	} else {
		t.Logf("%+v", resp)
		t.Logf("%+v", client.reply)
	}

}
