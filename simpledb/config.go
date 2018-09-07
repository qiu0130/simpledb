package simpledb

import (
	"time"
	"io/ioutil"
)

type Config struct {


	Addr string `yaml:"addr"`
	ReadTimeout time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	ConnectionTimeout time.Duration `yaml:"connection_timeout"`

	Password string `yaml:"password"`
	Database int `yaml:"database"`
	Servers []string `yaml:"servers,flow"`

	ServerIp string `yaml:"server_ip"`
	ServerPort string `yaml:"server_port"`

	MaxRetry int `yaml:"max_retry"`
	SleepTime time.Duration `yaml:"sleep_time"`
	MaxIdle int `yaml:"max_idle"`
	MaxActive int `yaml:"max_active"`
	IdleTimeout time.Duration `yaml:"idle_timeout"`

	DataType string `yaml:"data_type"`
	DataPath string `yaml:"data_path"`
	StaticDir string `yaml:"static_dir"`
	TemplateDir string `yaml:"template_dir"`

	LogPath string `yaml:"log_path"`
	LogName string `yaml:"log_name"`
	LogLevel string `yaml:"log_level"`
	Debug bool `yaml:"debug"`
}


func NewConfig(path string) (*Config, error) {

	var config Config
	if path == "" {
		path = "/Users/cowboy/golang/redis_monitor/src/conf/redis_monitor.yaml"
	}
	configData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(configData, &config); err != nil {
		return nil, err
	}
	log.Printf("%#+v\n", &config)
	return &config, nil
}
