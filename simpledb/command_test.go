package simpledb

import "testing"

func TestCheckCommand(t *testing.T) {

	var tests = []struct{
		command string
		key string
		value string
		limit int
		flag bool
	} {
		{"SET", "foo", "bar", 3, true},
	}
	for _, test := range tests {

		command, err := CheckCommand(test.command, test.key, test.value)
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%+v", command)
		}
	}
}
