package gores

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config contains the configuration parameters for running Gores
type Config struct {
	// Authetication for Redis connection
	RedisURL      string
	RedisPassword string

	// Maximum number of idle connections in the Redis pool
	RedisMaxIdle int

	// Redigo closes connection after it remains idle for this duration
	RedisIdleTimeout int

	// Conn blocks for this duration when trying to pop items from several queues from Redis
	BlpopMaxBlockTime int

	// Maximum number of workers needed for Gores
	MaxWorkers int

	// names of queues to fetch jobs from
	Queues []string

	// Dispatcher returns after it did not have jobs to dispatch for this duration
	DispatcherTimeout int

	// Worker returns after it did not have job to work on after this duration
	WorkerTimeout int
}

// InitConfig creates new config instance based on the config.json file path
func InitConfig(confPath string) (*Config, error) {
	config := Config{}

	configFile, err := os.Open(confPath)
	if err != nil {
		return &config, fmt.Errorf("init config failed: %s", err)
	}

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		return &config, fmt.Errorf("init config failed: %s", err)
	}

	return &config, nil
}
