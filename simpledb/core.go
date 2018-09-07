package simpledb

import (
	"time"
	"fmt"
	"net"
	"bufio"
	"log"
	"errors"
)

/*
		client ------ send msg ---------> server
        	^                               |
 			|						        |
			|					 	        |
			<-------- reply msg -------------
 */

 var (
 	keyErr = errors.New("invalid key, not a string")
 	KvErr = errors.New("empty kv")
 )

type SimpleServer struct {
	conn *Conn
	command *Command

	ConnectTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration

	rb *ReadBuffer
	wb *WriteBuffer
	host string
	port int

	argv int // input arguments of amount
	argc []*Resp  // input argument list
	reply []*Resp // reply result list
}

func init() {
	server := SimpleServer{}
}


func (s *SimpleServer) Run() {

}

func (s *SimpleServer) Close() error {
}

func (s *SimpleServer) Listen() error {

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("unable to listen on %v, %v\n", addr, err.Error())
	}
	log.Println("listen on: ", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err == nil && conn != nil {
			log.Println("accept from: ", addr)

			if s.writeTimeout == 0 {
				s.writeTimeout = defaultTimeout * time.Microsecond
			}
			if s.readTimeout == 0 {
				s.writeTimeout = defaultTimeout * time.Microsecond
			}
			conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			conn.SetReadDeadline(time.Now().Add(s.readTimeout))
			s.rb = &ReadBuffer{bufio.NewReader(conn), s.readTimeout}
			s.wb = &WriteBuffer{bufio.NewWriter(conn), s.writeTimeout}

			go handleProcess(s)
		}
		if err != nil {
			log.Println("accept err ", err)
			return err
		}
	}
}

func handleProcess(s *SimpleServer, args ...interface{}) {
	command, err := CheckCommand(args)
	if err != nil {
		s.wb.WriteArgs(err)
	}
	command.Process(s, args)
	s.reply

}


func (s *SimpleServer) Reply() {

	var reply []*Resp
	resp, err := s.rb.HandleStream()
	if err != nil {

	}
	switch resp.Type {
	case TypeString:
		reply = append(reply, resp)
	case TypeError:
		reply = append(reply, resp)
	case TypeInt:
		reply = append(reply, resp)
	case TypeBulkBytes:
		reply = append(reply, resp)
	case TypeArray:
		reply = append(reply, resp.Array...)
	default :

	}
	s.reply = reply // reserved server
	s.wb.WriteArgs(reply)
}


type Conn struct {
	conn *net.Conn
	readTimeout time.Duration
	writeTimeout time.Duration

	LeftTime time.Duration
}

var Kv map[string]interface{}

func Set(args ...interface{}) error {
	if Kv == nil {
		Kv = make(map[string]interface{})
	}
	key, ok := args[0].(string)
	if !ok {
		return keyErr
	}
	Kv[key] = args[1]
	return nil
}

func Get(s *SimpleServer, args ...interface{}) error {
	if Kv == nil {
		return KvErr
	}
	key, ok := args[0].(string)
	if !ok {
		return keyErr
	}
	if value, ok := Kv[key]; ok {
		s.wb.WriteArgs(value)
	}
	return KvErr

}

func (s *SimpleServer) lpush(args interface{}) {

}
func (s *SimpleServer) rpush(args interface{}) {

}
func (s *SimpleServer) lpop(arg interface{}) {

}
func (s *SimpleServer) rpop(arg interface{}) {

}

func (s *SimpleServer) lrem(arg interface{}) {

}
