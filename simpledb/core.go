package simpledb

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"simpledb/simpledb/config"
)

/*
		client ------ send msg ---------> server
        	^                               |
 			|						        |
			|					 	        |
			<-------- reply msg -------------
*/

/*
Queue commands:
	lpush, rpush, lpop, rpop, lrem, lindex, llen, lrange, lset, ltrim, rpoplpush, llfush

K/V commands:
	append, decr, decr, decrby, delete, exists, get, getset, incr, incrby, mdelete, mget, mset, mpop,
	msetex pop, set, setnx, setex, len, flush

Hash commands:
	hel, hexists, hget, hincrby, hkeys, hlen, hmget, hset, hset, hsetnx, hvals

Set commands:
	sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismenber, smembers, spop, srem, sunion, sunionstore

Schedule commands：
	add, read, schedule_flush, schedule_length

Misc:
	expire, info, flush_all, save_to_disk, restore_from_disk, merge_from_disk, client_quit, shutdown

 */

var (
	keyErr = errors.New("invalid key, not a string")
	KvErr  = errors.New("empty kv")
)
var serverConfig *config.Config

// k/v command
type Dict struct {
	mu    sync.RWMutex
	value map[string]interface{}
}

func (d *Dict) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.value)
}

func (d *Dict) set(k string, args interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.value[k] = args
}

func (d *Dict) get(k string) (interface{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if v, ok := d.value[k]; ok {
		return v, nil
	}
	return nil, KvErr
}

// hash command

// queue command

// set command

// zset command

type SimpleServer struct {
	conn    net.Conn
	command *Command
	dict    *Dict

	ConnectTimeout time.Duration
	readTimeout   time.Duration
	writeTimeout  time.Duration

	rb   *ReadBuffer
	wb   *WriteBuffer
	host string
	port int
}


func init() {
	var err error
	serverConfig, err = config.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *SimpleServer) Run() {
	server := NewServer()
	server.Run()
	s.listen()
}

func NewServer() *SimpleServer {

	return &SimpleServer{
		host: serverConfig.Server.Host,
		port: serverConfig.Server.Port,
		ConnectTimeout: serverConfig.Server.ConnectTimeout,
		readTimeout: serverConfig.Server.ReadTimeout,
		writeTimeout: serverConfig.Server.WriteTimeout,
	}
}

func (s *SimpleServer) Close() error {

	return s.conn.Close()
}

func (s *SimpleServer) listen() error {

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("unable to listen on %v, %v\n", addr, err.Error())
	}
	log.Println("listen on: ", addr)

	for {
		conn, err := listener.Accept()
		if err == nil && conn != nil {
			log.Printf("accept from: [%s][%s]", conn.RemoteAddr().Network(), conn.RemoteAddr().String())

			if s.writeTimeout == 0 {
				s.writeTimeout = defaultTimeout * time.Second
			}
			if s.readTimeout == 0 {
				s.writeTimeout = defaultTimeout * time.Second
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			conn.SetReadDeadline(time.Now().Add(s.readTimeout))
			s.rb = &ReadBuffer{bufio.NewReader(conn), s.readTimeout}
			s.wb = &WriteBuffer{bufio.NewWriter(conn), s.writeTimeout}
			s.conn = conn

			go handleProcess(s)
		}
		if err != nil {
			log.Println("accept err ", err)
			return err
		}
	}
}

func handleProcess(s *SimpleServer) {

	args, err := s.readResponse()
	if err != nil {
		log.Fatal(err)
	}

	if len(args) > 0 {
		if name, ok := args[0].(string); ok {
			command, err := CheckCommand(name, len(args)+1)
			if err != nil {
				s.wb.WriteArgs(err)
				s.wb.Flush()
				return
			}
			go command.Process(s, args)
		}
	}

}

func (s *SimpleServer) readResponse() (args []interface{}, err error) {

	resp, err := s.rb.HandleStream()
	if err != nil {
		return
	}
	switch resp.Type {
	case TypeString:
		args = append(args, string(resp.Value))
		return args, nil
	case TypeBulkBytes:
		args = append(args, string(resp.Value))
		return args, nil
	case TypeArray:
		arrays := resp.Array
		for _, element := range arrays {
			if element.Type == TypeArray {
				next, err := s.readResponse()
				if err != nil {
					return nil, err
				}
				args = append(args, next...)
			}
			args = append(args, element)
		}
	}
	return nil, fmt.Errorf("unknown type")
}

func (s *SimpleServer) writeArgs(args ...interface{}) (int, error) {
	return s.wb.WriteArgs(args)
}

func (s *SimpleServer) replyOk() (err error) {
	_, err = s.wb.WriteString("OK")
	if err != nil {
		return
	}
	err = s.wb.Flush()
	if err != nil {
		return
	}
	return
}

func (s *SimpleServer) flush() (err error) {
	return s.wb.Flush()
}

func Set(s *SimpleServer, args ...interface{}) error {
	if s.dict.value == nil {
		s.dict = &Dict{
			mu:    sync.RWMutex{},
			value: make(map[string]interface{}),
		}
	}

	key, ok := args[0].(string)
	if !ok {
		return keyErr
	}
	s.dict.set(key, args[1])

	return s.replyOk()
}

func Get(s *SimpleServer, args ...interface{}) error {
	if s.dict.value == nil {
		return KvErr
	}
	key, ok := args[0].(string)
	if !ok {
		return keyErr
	}
	value, err := s.dict.get(key)
	if err == nil {
		return err
	}
	s.writeArgs(value)
	return nil

}
