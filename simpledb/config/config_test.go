package config

import "testing"

func TestNewConfig(t *testing.T) {

	var tests = []struct{
		path string
		want string
	}{
		{path:"", want: "open : no such file or directory"},
		{path: "xxx", want: "open xxx: no such file or directory"},
		{path: "/home/qiu/Projects/simpledb/config/config.yaml", want: ""},
		{path: "/home/qiu/gopath/src/simpledb/simpledb/config/config.yaml", want: ""},
	}

	for _, test := range tests {
		config, err := NewConfig(test.path)
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%+v", config)
		}
	}
}

