package gores

import (
	"encoding/json"
	"errors"
	"os"
)

// Config contains the configuration parameters for running Gores
type Config struct {
	RedisURL          string
	RedisPassword     string
	BlpopMaxBlockTime int
	MaxWorkers        int
	Queues            []string
	DispatcherTimeout int
	WorkerTimeout     int
}

// InitConfig creates new config instance based on the config.json file path
func InitConfig(confPath string) (*Config, error) {
	config := Config{}

	configFile, err := os.Open(confPath)
	if err != nil {
		return &config, err
	}

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		return &config, errors.New("ERROR decode config.json")
	}
	return &config, nil
}
