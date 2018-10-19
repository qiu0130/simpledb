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
	register("LLEN", 1, 1, 'r', llen)
	register("LPUSH", 1, 1, 'r', lpush)
	register("LPOP", 1, 1, 'r', lpop)
	register("RPUSH", 1, 1, 'r', rpush)
	register("RPOP", 1, 1, 'r', rpop)
	register("LREM", 1, 1, 'r', lrem)
	register("LINDEX", 1, 1, 'r', lindex)
	register("LSET", 1, 1, 'r', lset)
	register("LRANGE", 1, 1, 'r', lrange)

	// hash command
	register("HDEL", 3, 1, 'w', hDel)
	register("HEXISTS", 3, 1, 'r', hExists)
	register("HGET", 3, 1, 'r', hGet)
	register("HSET", 4, 1, 'w', hSet)
	register("HGETALL", 2, 1, 'r', hGetAll)
	register("HKEYS", 2, 1, 'r', hKeys)
	register("HVALS", 2, 1, 'r', hVals)
	register("HLEN", 2, 1, 'r', hLen)
	register("HMGET", 3, 1, 'r', hMget)
	register("HMESET", 3, 1, 'w', hMSet)

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
