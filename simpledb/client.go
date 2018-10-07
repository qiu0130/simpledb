package simpledb

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
	"simpledb/simpledb/config"
	"log"
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
	log.Print(args)
	if name, ok := args[0].(string); ok {
		_, err := CheckCommand(name, len(args))
		if err != nil {
			return nil, err
		}
	}

	if err := c.connect(); err != nil {
		return nil, err
	}

	err := c.writeArgsWithFlush(args...)
	if err != nil {
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
	conn.SetWriteDeadline(time.Now().Add(c.writeTimeout*time.Second))
	conn.SetReadDeadline(time.Now().Add(c.readTimeout*time.Second))

	c.rb = &ReadBuffer{bufio.NewReader(conn), c.readTimeout}
	c.wb = &WriteBuffer{bufio.NewWriter(conn), c.writeTimeout}
	c.conn = conn
	return nil
}

func (c *Client) writeArgsWithFlush(args ...interface{}) (err error) {
	_, err = c.wb.WriteArgs(args...)
	if err != nil {
		return
	}
	err = c.wb.Flush()
	return
}

func (c *Client) readRely() (*Resp, error) {
	return c.rb.HandleStream()
}
// k/v command
func (c *Client) Set(key, value string) (*Resp, error) {
	return c.execute("SET", key, value)
}

func (c *Client) Get(key string) (*Resp, error) {
	return c.execute("GET", key)
}

func (c *Client) Delete(key, value string) (*Resp, error) {
	return c.execute("DEL", key, value)
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
	return c.execute("APPANDS", key, value)
}
func (c *Client) MGet(key, value string) (*Resp, error) {
	return c.execute("MGET", key, value)
}
func (c *Client) MSet(key, value string) (*Resp, error) {
	return c.execute("MSET", key, value)
}
func (c *Client) MDelete(key, value string) (*Resp, error) {
	return c.execute("MDELETE", key, value)
}

