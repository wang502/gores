package gores

import (
    "errors"
    "os"
    "encoding/json"
)

type Config struct {
    REDISURL string
    REDIS_PW string
    BLPOP_MAX_BLOCK_TIME int
    MAX_WORKERS int
    Queues []string
}

func InitConfig(confPath string) (*Config, error) {
    config := Config{}

    config_file, err := os.Open(confPath)
    if err != nil {
        return &config, err
    }

    decoder := json.NewDecoder(config_file)
    err = decoder.Decode(&config)
    if err != nil {
        return &config, errors.New("ERROR decode config.json")
    }
    return &config, nil
}
