package simpledb

import (
	"bufio"
	"fmt"
	"strconv"
	"time"
)
const (
	maxBulkLen = 100
)

type WriteBuffer struct {
	buf     *bufio.Writer
	timeout time.Duration
}

type ReadBuffer struct {
	buf     *bufio.Reader
	timeout time.Duration
}


func (w *WriteBuffer) Flush() error {
	return w.buf.Flush()
}

func (w *WriteBuffer) WriteInt64(i int64) (int, error) {
	return w.buf.WriteString(fmt.Sprintf(":%d\r\n", i))
}

func (w *WriteBuffer) WriteFloat64(f float64) (int, error) {
	return w.buf.WriteString(fmt.Sprintf(":%f\r\n", f))
}

func (w *WriteBuffer) WriteBulkString(s string) (int, error) {
	return w.buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func (w *WriteBuffer) WriteString(s string) (int, error) {
	return w.buf.WriteString(fmt.Sprintf("+%s\r\n", s))
}

func (w *WriteBuffer) WriteError(e error) (int, error) {
	return w.buf.WriteString(fmt.Sprintf("-%s", e.Error()))
}

func (w *WriteBuffer) WriteArray(i int) (int, error) {
	return w.buf.WriteString(fmt.Sprintf("*%d\r\n", i))
}

func (r *ReadBuffer) ReadLine() (RespType, []byte, error) {
	buf, err := r.buf.ReadBytes('\n')
	if err != nil {
		return 0, nil, err
	}
	return RespType(buf[0]), buf[1 : len(buf)-1], nil
}

func (r *ReadBuffer) HandleStream() (*Resp, error) {
	pos, buf, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch pos {
		// +Ok\r\n
	case TypeString:
		return NewString(buf), nil
		// -Error message\r\n
	case TypeError:
		return NewError(buf), nil
		// :10\r\n
	case TypeInt:
		return NewInt(buf), nil
		// $6\r\nfoobar\r\n
	case TypeBulkBytes:
		length, _ := strconv.Atoi(string(buf))
		if length < 1 {
			return NewBulkBytes([]byte("")), nil
		}
		p := make([]byte, length)
		_, err := r.buf.Read(p)
		if err != nil {
			return nil, err
		}
		return NewBulkBytes(p), nil
		// *3\r\n:1\r\n:2\r\n:3\r\n
	case TypeArray:
		length, _ := strconv.Atoi(string(buf))
		array := make([]*Resp, length)

		for i := 0; i < length; i++ {
			resp, err := r.HandleStream()
			if err != nil {
				return nil, err
			}
			array = append(array, resp)
		}
		return NewArray(array), nil

	default:
		return nil, fmt.Errorf("unkonwn type")
	}

}

func (w *WriteBuffer) WriteArgs(args ...interface{}) (int, error) {
	argv := len(args)
	if argv == 1 {
		switch arg := args[0].(type) {
		case int:
			return w.WriteInt64(int64(arg))
		case int64:
			return w.WriteInt64(arg)
		case string:
			if len(arg) > maxBulkLen {
				return w.WriteBulkString(arg)
			}
			return w.WriteString(arg)
		case []byte:
			if len(arg) > maxBulkLen {
				return w.WriteBulkString(string(arg))
			}
			return w.WriteString(string(arg))
		case bool:
			if arg {
				return w.WriteString("1")
			} else {
				return w.WriteString("0")
			}
		case float64:
			return w.WriteFloat64(arg)
		case float32:
			return w.WriteFloat64(float64(arg))
		case error:
			return w.WriteError(arg)
		default:
			return 0, fmt.Errorf("invalid type")
		}
	}
	if argv > 1 {
		w.WriteArray(argv)
		for _, arg := range args {
			w.WriteArgs(arg)
		}
	}
	return 0, fmt.Errorf("invalid type")
}
