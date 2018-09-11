package simpledb

import (
	"testing"
	"errors"
	"bytes"
	"bufio"
)



var (
	wb  *WriteBuffer
 	rb  *ReadBuffer
 	buf bytes.Buffer
 )


func init() {
	wb =  &WriteBuffer{}
	wb.buf = bufio.NewWriter(&buf)

	rb = &ReadBuffer{}

	rb.buf = bufio.NewReader(&buf)

}


func TestWriteBuffer_WriteInt64(t *testing.T) {

	var tests = []struct{
		i int64
		want string
	} {
		{100, ":100\r\n"},
		{200, ":200\r\n"},
		{100000, ":100000\r\n"},
	}

	for _, test := range tests {
		wb.WriteInt64(test.i)
		wb.Flush()
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.i, line, test.want, line == test.want)
		}
	}
}

func TestWriteBuffer_WriteFloat64(t *testing.T) {
	var tests = []struct{
		i float64
		want string
	} {
		{10.11, ":10.110000\r\n"},
		{20.11, ":20.110000\r\n"},
		{100.001, ":100.001000\r\n"},
	}

	for _, test := range tests {
		wb.WriteFloat64(test.i)
		wb.Flush()
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.i, line, test.want, line == test.want)
		}
	}

}

func TestWriteBuffer_WriteError(t *testing.T) {
	var tests = []struct{
		err error
		want string
	} {
		{errors.New("test"), "-test\r\n"},
		{errors.New("fail"), "-fail\r\n"},
	}

	for _, test := range tests {
		wb.WriteError(test.err)
		wb.Flush()
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.err, line, test.want, line == test.want)
		}
	}
}

func TestWriteBuffer_WriteString(t *testing.T) {
	var tests = []struct{
		str string
		want string
	} {
		{"test", "+test\r\n"},
		{"xxxxxxxxxxx", "+xxxxxxxxxxx\r\n"},
	}

	for _, test := range tests {
		wb.WriteString(test.str)
		wb.Flush()
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.str, line, test.want, line == test.want)
		}
	}

}

func TestWriteBuffer_WriteArray(t *testing.T) {
	var tests = []struct{
		i int
		want string
	} {
		{1, "*1\r\n"},
		{100, "*10r\n"},
	}

	for _, test := range tests {
		wb.WriteArray(test.i)
		wb.Flush()
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.i, line, test.want, line == test.want)
		}
	}
}

func TestWriteBuffer_WriteBulkString(t *testing.T) {
	var tests = []struct{
		str string
		want string
	} {
		{"ok", "$2\r\nok\r\n"},
		{"xxx", "$3\r\nxxx\r\n"},
	}

	for _, test := range tests {
		wb.WriteBulkString(test.str)
		wb.Flush()

		p := make([]byte, 16*2014)
		n, err := buf.Read(p)
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.str, string(p[:n]), test.want, string(p[:n]) == test.want)
		}
	}
}

func TestWriteBuffer_WriteArgs(t *testing.T) {

	var tests = []struct{
		args interface{}
		want string
	} {
		{int(1), ":1\r\n"},
		{int64(10), ":10\r\n"},

		{"ok", "+ok\r\n"},
		{"xxxxx", "$5\r\nxxxxx\r\n"},

		{errors.New("fail"), "-fail\r\n"},
		{float32(10), ":10.000000\r\n"},
		{float64(100), ":100.000000\r\n"},

		{[]byte("yes"), "+yes\r\n"},
		{[]byte("xxxxx"), "$5\r\nxxxxx\r\n"},

	}

	for _, test := range tests {
		wb.WriteArgs(test.args)
		wb.Flush()

		p := make([]byte, 16*2014)
		n, err := buf.Read(p)
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, %q, result: %v", test.args, string(p[:n]), test.want, string(p[:n]) == test.want)
		}
	}

	wb.WriteArgs(1, 100, "ok", "xxxxx", []byte("yyyyy"))
	wb.Flush()
	want := "*5\r\n:1\r\n:100\r\n+ok\r\n$5\r\nxxxxx\r\n$5\r\nyyyyy\r\n"
	p := make([]byte, 16*2014)
	n, err := buf.Read(p)
	if err != nil {
		t.Error(err)
	} else {
		t.Logf("1 100 ok, xxxxx, yyyyy; %q, %q, %v",  want, string(p[:n]), want == string(p[:n]))
	}


}


func TestReadBuffer_ReadLine(t *testing.T) {


	var tests = []struct {
		data string
		want string
	} {
		{
		":100\r\n", "100"	,
		},
		{
			"+ok\r\n", "ok"	,
		},
		{
			"$5\r\n", "5",
		},
	}

	for _, test := range tests {
		buf.WriteString(test.data)
		pos, buf, err := rb.ReadLine()
		if err != nil {
			t.Error(err)
		} else {
			t.Logf("%v, %q, result: %v", pos, string(buf), string(buf) == test.want)
		}
	}
}

func TestReadBuffer_HandleStream(t *testing.T) {
	var tests = []struct {
		data string
		want string
	} {
		{
			":100\r\n", "100"	,
		},
		{
			"+ok\r\n", "ok"	,
		},
		{
			"$5\r\nxxxxx\r\n", "xxxxx",
		},
		{
			"*2\r\n+ok\r\n:100\r\n", "ok",
		},

	}
	for _, test := range tests {
		buf.WriteString(test.data)
		resp, err := rb.HandleStream()

		if err != nil {
			t.Error(err)
		} else {
			if resp.Type == TypeArray {
				for _, arr := range resp.Array {
					t.Logf("%v, %q", arr, string(arr.Value))
				}

			} else {
				t.Logf("%v, %q, result: %v", resp, string(resp.Value), string(resp.Value) == test.want)
			}
		}
	}
}

