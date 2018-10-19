package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

const (
	defaultConfigPath = "/Users/cowboy/golang/src/simpledb/simpledb/config/config.yaml"
	//defaultConfigPath = "/home/qiu/gopath/src/simpledb/simpledb/config/config.yaml"
)

type Config struct {
	Server struct {
		Host           string        `yaml:"host"`
		Port           int           `yaml:"port"`
		ReadTimeout    time.Duration `yaml:"read_timeout"`
		WriteTimeout   time.Duration `yaml:"write_timeout"`
		ConnectTimeout time.Duration `yaml:"connect_timeout"`
	} `yaml:"server"`

	Client struct {
		Host           string        `yaml:"host"`
		Port           int           `yaml:"port"`
		ReadTimeout    time.Duration `yaml:"read_timeout"`
		WriteTimeout   time.Duration `yaml:"write_timeout"`
		ConnectTimeout time.Duration `yaml:"connect_timeout"`
	} `yaml:"client"`
}

func NewConfig(path ...string) (*Config, error) {

	var (
		curPath string
		config  Config
	)
	if len(path) == 0 {
		curPath = defaultConfigPath
	} else if len(path) > 0 {
		curPath = path[0]
	} else {
		return nil, fmt.Errorf("invalid config path")
	}
	file, err := ioutil.ReadFile(curPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(file, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
