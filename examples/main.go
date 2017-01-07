package main

import (
    "strings"
    "log"
    "flag"
    "time"
    "github.com/wang502/gores/gores"
    "github.com/wang502/gores/examples/tasks"
)

func Produce(config *gores.Config){
    item := map[string]interface{}{
      "Name": "Rectangle",
      "Queue": "TestJob",
      "Args": map[string]interface{}{
                "Length": 10,
                "Width": 10,
              },
      "Enqueue_timestamp": time.Now().Unix(),
    }

    resq := gores.NewResQ(config)
    err := resq.Enqueue(item)
    if err != nil {
        log.Fatalf("ERROR Enqueue item to ResQ")
    }
}

func Consume(config *gores.Config){
    tasks := map[string]interface{}{
                "Item": tasks.PrintItem,
                "Rectangle": tasks.CalculateArea,
           }
    err := gores.Launch(config, &tasks)
    if err != nil {
        log.Fatalf("ERROR launching consumer: %s\n", err)
    }
}

func main()  {
    configPath := flag.String("c", "config.json", "path to configuration file")
    option := flag.String("o", "consume", "option: weither produce or consume")
    flag.Parse()

    config, err := gores.InitConfig(*configPath)
    if err != nil {
        log.Fatalf("Cannot read config file")
    }

    if strings.Compare("produce", *option) == 0 {
        Produce(config)
    } else {
        Consume(config)
    }
}
