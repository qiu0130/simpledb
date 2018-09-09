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
	invalidCommand = errors.New("invalid command")
)

// handle command process, error mean the function runtime error
type CommandProcess func(s *SimpleServer, args ...interface{}) error

type Command struct {
	Name    string
	Arity   int            // limit argument, > N
	Flag    int            // client or server
	SFlag   string         // r, w, a
	Process CommandProcess // handle command function
}

var CommandTable []*Command

func init() {
	register("SET", 3, 1, Set)
	register("GET", 2, 0, Get)
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

func CheckCommand(args ...interface{}) (*Command, error) {

	commandLen := len(args)
	if commandLen == 0 {
		return nil, emptyCommand
	}
	if name, ok := args[0].(string); ok {
		command := LookupCommand(name)
		if command == nil {
			return nil, lackCommand
		}
		if commandLen != command.Arity {
			return nil, lackCommand
		}
		return command, nil
	}

	return nil, invalidCommand
}
