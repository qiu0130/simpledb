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
	invalidCommand = errors.New("lack of arguments")
)

// handle command process, error mean the function runtime error
type CommandProcess func(s *Server, resp *Resp) error

type Command struct {
	Name    string
	Arity   int            // limit argument, > N
	Flag    int            // client or server
	SFlag   string         // r, w, a
	Process CommandProcess // handle command function
}

var CommandTable []*Command

func init() {
	register("set", 3, 1, set)
	register("get", 2, 0, get)

	register("decr", 2, 1, decrease)
	register("decrby", 3, 1, decreaseBy)
	register("incr", 2, 1, increase)
	register("incrBy", 3, 1, increaseBy)

	register("appends", 3, 1, appends)

}

func register(name string, arity int, flag int, process CommandProcess) {
	var sFlag string
	switch flag {
	case R:
		sFlag = "r"
	case W:
		sFlag = "w"
	case A:
		sFlag = "a"
	}
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
	if arity != command.Arity {
		return nil, invalidCommand
	}
	return command, nil

}
