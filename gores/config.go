package gores

import (
    "errors"
    "os"
    "path"
    "runtime"
    "encoding/json"
)

type Config struct {
    REDISURL string
    REDIS_PW string
    BLPOP_MAX_BLOCK_TIME int
    MAX_WORKERS int
    Queues []string
}

func InitConfig() (*Config, error) {
    config := Config{}

    _, filename, _, _ := runtime.Caller(0)
    config_file, err := os.Open(path.Dir(filename) + "/config.json")
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
