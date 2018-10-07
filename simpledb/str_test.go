package simpledb

import (
	"testing"
)


var d *Dict


func init() {
	d = newDict()
}

func TestDict(t *testing.T) {

	d.add("foo", "bar")
	d.add("test1", "hh")
	d.add("a", "v")
	d.add("zz", 100)

	size := d.size()
	t.Logf("size: %d, expected: 4", size)
	getValue, err := d.get("foo")
	if err != nil {
		t.Error(err)
	}
	t.Logf("value: %s, expected: bar", getValue)
	d.delete("test1")
	getValue, err = d.get("test1")
	if err != nil {
		t.Error(err)
	}
	t.Logf("value: %s, expected: ERR value is empty", getValue)

	size = d.size()
	t.Logf("size: %d, expected: 3", size)



}


