package simpledb

import (
	"net"
	"time"
	"strconv"
	"bufio"
	"fmt"
)

const (
	defaultTimeout = 1000
)



type Client struct {
	Host string
	Port int
	conn *Conn

	rb *ReadBuffer
	wb *WriteBuffer
	ConnectTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration

	command *Command
	argv int
	argc *Resp
	reply *Resp
}


func init() {


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

func New(host string, port int, timeout time.Duration) *Client {

	return &Client{
		Host: host,
		Port: port,
		ConnectTimeout: timeout,
		readTimeout: timeout,
		writeTimeout: timeout,
	}
}

func (c *Client) execute(args ...interface{}) (*Resp, error) {
	// lookup commandTable and
	// check argument of quantity
	command, err := CheckCommand(args)
	if err != nil {
		return nil, err
	}

	if err = c.connect(); err != nil {
		return nil, err
	}

	_, err = c.writeArgs(command.Name, args)
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
		c.writeTimeout = defaultTimeout * time.Microsecond
	}
	if c.readTimeout == 0 {
		c.writeTimeout = defaultTimeout * time.Microsecond
	}
	conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	conn.SetReadDeadline(time.Now().Add(c.readTimeout))

	c.rb = &ReadBuffer{bufio.NewReader(conn), c.readTimeout}
	c.wb = &WriteBuffer{bufio.NewWriter(conn), c.writeTimeout}
	return nil
}

func (c *Client) writeArgs(args ...interface{}) (int, error) {
	return 	c.wb.WriteArgs(args)
}
func (c *Client) readRely() (*Resp, error) {
	return c.rb.HandleStream()
}



func (c *Client) Set(args ...interface{}) {
	c.execute(args)
}

func (c *Client) Get(args ...interface{}) {
	c.execute(args)
}

func (c *Client) Lpush(args ...interface{}) {
	c.execute()
}

