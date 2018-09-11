package simpledb

import "testing"

func TestNewClient(t *testing.T) {
	client := NewClient()

	resp, err := client.Set("foo", "bar")
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
