package main

import (
    "flag"
    "log"
    "time"
    "github.com/wang502/gores/gores"
)

var (
    configPath = flag.String("c", "config.json", "path to configuration file")
)

func main(){
  flag.Parse()

  config, err := gores.InitConfig(*configPath)
  if err != nil {
      log.Fatalf("Cannot read config file")
  }

  resq := gores.NewResQ(config)

  args := map[string]interface{}{
                "Length": 10,
                "Width": 10,
          }
  item := map[string]interface{}{
    "Name": "Rectangle",
    "Queue": "TestJob",
    "Args": args,
    "Enqueue_timestamp": time.Now().Unix(),
  }

  err = resq.Enqueue(item)
  if err != nil {
      log.Fatalf("ERROR Enqueue item to ResQ")
  }
}
