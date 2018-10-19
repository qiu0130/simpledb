package simpledb

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"simpledb/simpledb/config"
	"strconv"
	"time"
)

const (
	defaultTimeout = 3
)

var clientConfig *config.Config

type Client struct {
	Host string
	Port int
	conn net.Conn

	rb             *ReadBuffer
	wb             *WriteBuffer
	ConnectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration

	command *Command
	reply   *Resp
}

func init() {
	var err error
	clientConfig, err = config.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
}
func DefaultClient() *Client {

	cli := &Client{}
	cli.Host = "127.0.0.1"
	cli.Port = 9000
	cli.ConnectTimeout = 10
	cli.readTimeout = 10
	cli.writeTimeout = 10
	return cli
}

func NewClient() *Client {

	return &Client{
		Host:           clientConfig.Client.Host,
		Port:           clientConfig.Client.Port,
		ConnectTimeout: clientConfig.Client.ConnectTimeout,
		readTimeout:    clientConfig.Client.ReadTimeout,
		writeTimeout:   clientConfig.Client.WriteTimeout,
	}
}

func (c *Client) Close() error {
	return c.Close()
}

func (c *Client) execute(args ...interface{}) (*Resp, error) {
	// lookup commandTable and
	// check argument of quantity
	var (
		arity int
	)
	log.Printf("execte command: %s, args: %v", args[0], args)
	if len(args) == 0 {
		return nil, fmt.Errorf("args is empty")
	}

	cmd, _ := args[0].(string)
	for _, arg := range args {
		switch t := arg.(type) {
		case []string:
			arity += len(t)
		case []byte:
			arity += len(t)
		case map[string]interface{}:
			arity += len(t) * 2
		default:
			arity += 1
		}
	}

	if _, err := CheckCommand(cmd, arity); err != nil {
		return nil, err
	}
	if err := c.connect(); err != nil {
		return nil, err
	}

	if err := c.writeArgsWithFlush(args...); err != nil {
		return nil, fmt.Errorf("conn write buffer fail %s", err.Error())
	}

	reply, err := c.readRely()
	if err != nil {
		return nil, fmt.Errorf("reply read buffer fail %s", err.Error())
	}
	c.reply = reply
	return reply, nil
}

func (c *Client) connect() error {

	addr := c.Host + ":" + strconv.Itoa(c.Port)
	conn, err := net.DialTimeout("tcp", addr, c.ConnectTimeout*time.Second)
	if err != nil {
		return fmt.Errorf("connect addr %s fail %s", addr, err.Error())
	}
	if c.writeTimeout == 0 {
		c.writeTimeout = defaultTimeout
	}
	if c.readTimeout == 0 {
		c.writeTimeout = defaultTimeout
	}
	conn.SetWriteDeadline(time.Now().Add(c.writeTimeout * time.Second))
	conn.SetReadDeadline(time.Now().Add(c.readTimeout * time.Second))

	c.rb = &ReadBuffer{bufio.NewReader(conn), c.readTimeout}
	c.wb = &WriteBuffer{bufio.NewWriter(conn), c.writeTimeout}
	c.conn = conn
	return nil
}

func (c *Client) writeArgsWithFlush(args ...interface{}) (err error) {

	flush := func() {
		_, err = c.wb.WriteArgs(args...)
		if err != nil {
			return
		}
		err = c.wb.Flush()
	}
	respPrint := func(isActive bool) {
		if isActive {
			var buf bytes.Buffer
			c.wb.buf = bufio.NewWriter(&buf)

			flush()
			b := make([]byte, 1024*4)
			n, err := buf.Read(b)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("RESP: %q\n", string(b[:n]))
		} else {
			flush()
		}
	}
	respPrint(false)
	return
}

func (c *Client) readRely() (*Resp, error) {
	return c.rb.HandleStream()
}

// str command
func (c *Client) Set(key, value string) (*Resp, error) {
	return c.execute("SET", key, value)
}

func (c *Client) Get(key string) (*Resp, error) {
	return c.execute("GET", key)
}

func (c *Client) Delete(key ...string) (*Resp, error) {
	return c.execute("DEL", key)
}

func (c *Client) Exists(key string) (*Resp, error) {
	return c.execute("EXISTS", key)
}
func (c *Client) Incr(key string) (*Resp, error) {
	return c.execute("INCR", key)
}
func (c *Client) IncrBy(key string, value int64) (*Resp, error) {
	return c.execute("INCRBY", key, value)
}
func (c *Client) Decr(key string) (*Resp, error) {
	return c.execute("DECR", key)
}
func (c *Client) DecrBy(key string, value int64) (*Resp, error) {
	return c.execute("DECRBY", key, value)
}
func (c *Client) Append(key, value string) (*Resp, error) {
	return c.execute("APPEND", key, value)
}
func (c *Client) MGet(key ...string) (*Resp, error) {
	return c.execute("MGET", key)
}
func (c *Client) MSet(value map[string]interface{}) (*Resp, error) {
	return c.execute("MSET", value)
}

// list command

func (c *Client) Llen(value string) (*Resp, error) {
	return c.execute("LLEN", value)
}

func (c *Client) Lpush(key, value string) (*Resp, error) {
	return c.execute("LPUSH", key, value)
}

func (c *Client) Lpop(key string) (*Resp, error) {
	return c.execute("LPOP", key)
}

func (c *Client) Rpush(key, value string) (*Resp, error) {
	return c.execute("RPUSH", key, value)
}

func (c *Client) Rpop(key string) (*Resp, error) {
	return c.execute("RPOP", key)
}

func (c *Client) Lrem(key string) (*Resp, error) {
	return c.execute("LREM", key)
}

func (c *Client) Lindex(key string, index int) (*Resp, error) {
	return c.execute("LINDEX", key, index)
}

func (c *Client) Lset(key string, index int, value string) (*Resp, error) {
	return c.execute("LSET", key, index, value)
}
func (c *Client) Lrange(key string, start, stop int) (*Resp, error) {
	return c.execute("LRANGE", key, start, stop)
}

// hash command
func (c *Client) HDel(value ...string) (*Resp, error) {
	return c.execute("HDEL", value)
}
func (c *Client) HExists(key, field string) (*Resp, error) {
	return c.execute("HEXISTS", key, field)
}
func (c *Client) HGet(key string, start, stop int) (*Resp, error) {
	return c.execute("HGET", key, start, stop)
}
func (c *Client) HSet(key, field string, value interface{}) (*Resp, error) {
	return c.execute("HSET", key, field, value)
}
func (c *Client) HGetAll(key string, start, stop int) (*Resp, error) {
	return c.execute("HGETALL", key, start, stop)
}
func (c *Client) HKeys(key string, start, stop int) (*Resp, error) {
	return c.execute("HKEYS", key, start, stop)
}
func (c *Client) HVals(key string, start, stop int) (*Resp, error) {
	return c.execute("HVAlS", key, start, stop)
}
func (c *Client) HLen(key string) (*Resp, error) {
	return c.execute("HLEN", key)
}
func (c *Client) HMGet(value ...string) (*Resp, error) {
	return c.execute("HMGET", value)
}
func (c *Client) HMSet(key string, value map[string]interface{}) (*Resp, error) {
	return c.execute("HMSET", key, value)
}
