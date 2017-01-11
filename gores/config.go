package gores

import (
    "errors"
    "os"
    "encoding/json"
)

// Config contains the configuration parameters for running Gores
type Config struct {
    REDISURL string
    REDIS_PW string
    BLPOP_MAX_BLOCK_TIME int
    MAX_WORKERS int
    Queues []string
    DispatcherTimeout int
    WorkerTimeout int
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
