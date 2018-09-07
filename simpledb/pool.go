package simpledb

import (
	"time"
	"net"
)

type Pool struct {

	MaxConn uint32
	IdleConn uint32

	LefTimeConn time.Duration
	FreeConn []*net.Conn
}

