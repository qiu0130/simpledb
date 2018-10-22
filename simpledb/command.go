package simpledb

import (
	"errors"
	"strings"
)

const (
	R = iota // only read command
	W        // only write command
	A        // only admin command
)

var (
	emptyCommand   = errors.New("empty command")
	lackCommand    = errors.New("lack of command")
	invalidCommand = errors.New("invalid of command")
)

// handle command process, error mean the function runtime error
type CommandProcess func(s *Server, resp *Resp) error

type Command struct {
	Name    string
	Arity   int            // limit argument, > N
	Flag    int            // client or server
	SFlag   byte           // r, w, a
	Process CommandProcess // handle command function
}

var CommandTable []*Command

func init() {
	// str command
	register("SET", 3, 1, 'w', set)
	register("GET", 2, 1, 'r', get)
	register("DEL", 2, 1, 'w', deletes)
	register("EXISTS", 2, 1, 'r', exists)
	register("DECR", 2, 1, 'w', decrease)
	register("DECRBY", 3, 1, 'w', decreaseBy)
	register("INCR", 2, 1, 'w', increase)
	register("INCRBY", 3, 1, 'w', increaseBy)
	register("APPEND", 3, 1, 'a', appends)
	register("MSET", 3, 1, 'w', multipleSet)
	register("MGET", 2, 1, 'w', multipleGet)

	// list command
	register("LLEN", 1, 1, 'r', lLen)
	register("LPUSH", 1, 1, 'r', lPush)
	register("LPOP", 1, 1, 'r', lPop)
	register("RPUSH", 1, 1, 'r', rPush)
	register("RPOP", 1, 1, 'r', rPop)
	register("LREM", 1, 1, 'r', lRem)
	register("LINDEX", 1, 1, 'r', lIndex)
	register("LSET", 1, 1, 'r', lSet)
	register("LRANGE", 1, 1, 'r', lRange)

	// hash command
	register("HDEL", 3, 1, 'w', hDel)
	register("HEXISTS", 3, 1, 'r', hExists)
	register("HGET", 3, 1, 'r', hGet)
	register("HSET", 4, 1, 'w', hSet)
	register("HGETALL", 2, 1, 'r', hGetAll)
	register("HKEYS", 2, 1, 'r', hKeys)
	register("HVALS", 2, 1, 'r', hVals)
	register("HLEN", 2, 1, 'r', hLen)
	register("HMGET", 3, 1, 'r', hMGet)
	register("HMESET", 3, 1, 'w', hMSet)

	// set command
	register("SADD", 3, 1, 'w', sAdd)
	register("SCARD", 2, 1, 'r', sCard)
	register("SDIFF", 3, 1, 'r', sDiff)
	register("SDIFFSCORE", 3, 1, 'r', sDiffScore)
	register("SINTER", 3, 1, 'r', sInter)
	register("SINTERSCORE", 3, 1, 'r', sInterScore)
	register("SUNION", 3, 1, 'r', sUnion)
	register("SUNIONSCORE", 3, 1, 'w', sUnionScore)
	register("SISMEMBER", 3, 1, 'r', sIsMember)
	register("SMEMBERS", 2, 1, 'r', sMembers)
	register("SREM", 3, 1, 'w', sRem)

	// sorted set command
	register("ZADD", 4, 1, 'w', zAdd)
	register("ZCARD", 2, 1, 'r', zCard)
	register("ZROUNT", 4, 1, 'r', zCount)
	register("ZINCRBY", 4, 1, 'w', zIncrementBy)
	register("ZRANGE", 4, 1, 'r', zRange)
	register("ZRANGEBYSCORE", 4, 1, 'r', zRangeByScore)
	register("ZRANK", 3, 1, 'r', zRank)
	register("ZREM", 3, 1, 'w', zRem)

}

func register(name string, arity int, flag int, sFlag byte, process CommandProcess) {
	c := &Command{name, arity, flag, sFlag, process}
	CommandTable = append(CommandTable, c)
}

func LookupCommand(name string) *Command {

	UpperName := strings.ToUpper(name)
	for _, command := range CommandTable {
		if command.Name == UpperName {
			return command
		}
	}
	return nil
}

func CheckCommand(name string, arity int) (*Command, error) {

	if name == "" {
		return nil, emptyCommand
	}
	command := LookupCommand(name)
	if command == nil {
		return nil, lackCommand
	}
	if arity >= command.Arity {
		return command, nil
	}
	return nil, invalidCommand

}
