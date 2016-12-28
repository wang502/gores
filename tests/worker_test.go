package tests

import (
    "testing"
    "os"
    "github.com/deckarep/golang-set"
    "github.com/wang502/gores/gores"
)

func TestNewWorker(t *testing.T){
    server := os.Getenv("REDISURL")
    password := os.Getenv("REDIS_PW")
    queues := mapset.NewSet()
    worker := gores.NewWorker(server, password, queues)

    if worker == nil {
        t.Errorf("NewWorker() ERROR")
    }
}
