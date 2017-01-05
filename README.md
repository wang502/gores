# Gores
[![Build Status]https://travis-ci.com/wang502/gores.svg?token=KeHkjMsksZ2RWDDg6h5k&branch=master](https://travis-ci.org/wang502/gores)

An asynchronous job execution system with Redis as message broker

## Installation
Get the package
```
go get github.com/wang502/gores/gores
```
Import the package
```go
import "github.com/wang502/gores/gores"
```

## Usage
### Configuration
Add a config.json in your project folder
```json
{
  "REDISURL": "127.0.0.1:6379",
  "REDIS_PW": "mypassword",
  "BLPOP_MAX_BLOCK_TIME" : 1,
  "MAX_WORKERS": 2,
  "Queues": ["queue1", "queue2"]
}
```
- REDISURL: specifies the Redis server address. If you run in a local Redis, the dafault host is ```127.0.0.1:6379```
- REDIS_PW: specifies Redis password
- BLPOP_MAX_BLOCK_TIME: time to block when calling BLPOP command in Redis
- MAX_WORKERS: maximum number of concurrent worker, each worker is a goroutine
- Queues: array of queue names on Redis message broker

### Enqueue item to message broker
```go
// produce.go

configPath := flag.String("c", "config.json", "path to configuration file")
flag.Parse()
config, err := gores.InitConfig(*configPath)

resq := gores.NewResQ(config)
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
package tasks

// task for item with 'Name' = 'Rectangle'
func CalculateArea(item map[string]interface{}) error {
    var err error

    length := item["Length"]
    width := item["Width"]
    if length == nil || width == nil {
        err = errors.New("Map has no required attributes")
        return err
    }
    fmt.Printf("The area is %d\n", int(length.(float64)) * int(width.(float64)))
    return err
}
```

### Worker that execute tasks
```go
// consume.go

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
The rectangle area is 100
```
