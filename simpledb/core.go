package simpledb

import (
	"errors"
	"simpledb/simpledb/config"
	"net"
	"time"
	"log"
	"fmt"
	"bufio"
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
	append, decr, decrby, delete, exists, get, getset, incr, incrby, mdelete, mget, mset, mpop,
	msetex, set, setnx, setex, len, flush

Hash commands:
	hel, hexists, hget, hincrby, hkeys, hlen, hmget, hsmet, hset, hsetnx, hvals

Set commands:
	sadd, scard, sdiff, sdiffstore, sinter, sinterstore, sismenber, smembers, spop, srem, sunion, sunionstore

SortedSet commands:
	zadd, zcard, zcount, zincrby, zrange, zrangebysocre, zrank, zrem, zremrangebyrank...

Misc:
	expire, info, flush_all, save_to_disk, restore_from_disk, merge_from_disk, client_quit, shutdown

 */

var (

	empty = errors.New("ERR value is empty")
	errStr = errors.New("ERR value not a string")
	errInteger = errors.New("ERR value not a integer or out of range")

)
var serverConfig *config.Config

const (
	defaultDictSize = 1024
	defaultHashSize = 1024
	defaultSortedSetSize = 1024
)



type Server struct {
	conn    net.Conn
	command *Command
	dict    *Dict
	hash []Hash
	queue *Queue

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


func NewServer() *Server {

	return &Server{
		host: serverConfig.Server.Host,
		port: serverConfig.Server.Port,
		ConnectTimeout: serverConfig.Server.ConnectTimeout,
		readTimeout: serverConfig.Server.ReadTimeout,
		writeTimeout: serverConfig.Server.WriteTimeout,
	}
}

func (s *Server) Run() {
	server := NewServer()
	server.listen()
}

func (s *Server) Close() error {
	return s.conn.Close()
}

func (s *Server) listen() error {

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
				s.writeTimeout = defaultTimeout
			}
			if s.readTimeout == 0 {
				s.writeTimeout = defaultTimeout
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout*time.Second))
			conn.SetReadDeadline(time.Now().Add(s.readTimeout*time.Second))
			s.rb = &ReadBuffer{bufio.NewReader(conn), s.readTimeout}
			s.wb = &WriteBuffer{bufio.NewWriter(conn), s.writeTimeout}
			s.conn = conn

			go handleProcess(s)
		}
		if err != nil {
			log.Fatal("accept err: ", err)
			return err
		}
	}
}

func handleProcess(s *Server) {

	resp, err := s.rb.HandleStream()
	if err != nil {
		log.Fatal(err)
	}
	if resp.Type == TypeArray {
		arity := len(resp.Array)
		name := string(resp.Array[0].Value)
		log.Print(name, arity)
		command, err := CheckCommand(name, arity)
		if err != nil {
			log.Fatal(err)
		}
		go command.Process(s, resp)
	} else {
		s.writeArgs(resp.Value)
	}

}

func (s *Server) writeArgs(args ...interface{}) (err error) {
	_, err = s.wb.WriteArgs(args...)
	if err != nil {
		return
	}
	err = s.flush()
	return
}

func (s *Server) replyOk() (err error) {
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

func (s *Server) reply0() (err error) {
	_, err = s.wb.WriteString("0")
	if err != nil {
		return
	}
	err = s.wb.Flush()
	if err != nil {
		return
	}
	return
}

func (s *Server) reply1() (err error) {
	_, err = s.wb.WriteString("1")
	if err != nil {
		return
	}
	err = s.wb.Flush()
	if err != nil {
		return
	}
	return
}


func (s *Server) replyNil() (err error) {
	_, err = s.wb.WriteString("nil")
	if err != nil {
		return
	}
	err = s.wb.Flush()
	if err != nil {
		return
	}
	return
}

func (s *Server) replyErr(errs error) (err error) {
	_, err = s.wb.WriteArgs(errs)
	if err != nil {
		return
	}
	err = s.wb.Flush()
	if err != nil {
		return
	}
	return
}


func (s *Server) flush() (err error) {
	return s.wb.Flush()
}



