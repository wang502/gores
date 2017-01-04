# Gores
An asynchronous job execution system with Redis as message broker

## Installation
```
go get github.com/wang502/gores/gores
```

## Usage
### Configuration
Add a config.json in your project folder
```json
{
  "REDISURL": "",
  "REDIS_PW": "",
  "BLPOP_MAX_BLOCK_TIME" : 1,
  "MAX_WORKERS": 2,
  "Queues": ["queue1", "queue2"]
}
```
- REDISURL: specifies the Redis server address
- REDIS_PW: specifies Redis password
- BLPOP_MAX_BLOCK_TIME: time to block when calling BLPOP command in Redis
- MAX_WORKERS: maximum number of concurrent worker, each worker is a goroutine
- Queues: array of queue names on Redis message broker

### Enqueue item to message broker
```go
// produce.go

import (
    "github.com/wang502/gores/gores"
    "log"
    "flag"
)

configPath := flag.String("c", "config.json", "path to configuration file")
flag.Parse()
config, err := gores.InitConfig(*configPath)

resq := gores.NewResQ(config)
args := map[string]interface{}{
              "Length": 10,
              "Width": 10,
        }
item := map[string]interface{}{
  "Name": "Rectangle",
  "Queue": "TestJob",
  "Args": map[string]interface{}{
                "Length": 10,
                "Width": 10,
          },
  "Enqueue_timestamp": time.Now().Unix(),
}

err = resq.Enqueue(item)
if err != nil {
	log.Fatalf("ERROR Enqueue item to ResQ")
}
```

```
go run produce.go ./config.json
```

### Define tasks
```go
// task for item with 'Name' = 'Rectangle'
func CalculateArea(item map[string]interface{}) error {
    var err error

    length := item["Length"]
    width := item["Width"]
    if length == nil || width == nil {
        err = errors.New("Map has no required attributes")
        return err
    }
    fmt.Println("The area is %d", int(length.(float64)) * int(width.(float64)))
    return err
}
```

### Worker that execute tasks
```go
// consume.go

import (
    "github.com/wang502/gores/gores"
    "log"
    "flag"
)

flag.Parse()
config, err := gores.InitConfig(*configPath)

tasks := map[string]interface{}{
              "Item": tasks.PrintItem,
              "Rectangle": tasks.CalculateArea,
         }
gores.Launch(config, &tasks)
```

```
$ go run consume.go ./config.json
```

### Output
```
The area is 100
```
