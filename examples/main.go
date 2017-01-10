package main

import (
    "strings"
    "log"
    "flag"
    "fmt"
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
    if resq == nil {
        log.Fatalf("resq is nil")
    }
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

func GetInfo(config *gores.Config) map[string]interface{}{
    resq := gores.NewResQ(config)
    if resq == nil {
        log.Fatalf("resq is nil")
    }
    info := resq.Info()
    return info
}

func main()  {
    configPath := flag.String("c", "config.json", "path to configuration file")
    option := flag.String("o", "consume", "option: weither produce or consume")
    flag.Parse()

    config, err := gores.InitConfig(*configPath)
    if err != nil {
        log.Fatalf("Cannot read config file: %s", err)
    }

    if strings.Compare("produce", *option) == 0 {
        Produce(config)
    } else if strings.Compare("consume", *option) == 0 {
        Consume(config)
    } else if strings.Compare("info", *option) == 0 {
        info := GetInfo(config)

        fmt.Println("Gores Info: ")
        for k, v := range info {
            switch v.(type) {
            case string:
              fmt.Printf("%s : %s\n", k, v)
            case int:
              fmt.Printf("%s : %d\n", k, v)
            case int64:
              fmt.Printf("%s : %d\n", k, v)
            }
        }
    }
}
