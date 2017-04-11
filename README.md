# Gores [![Build Status](https://travis-ci.com/wang502/gores.svg?token=KeHkjMsksZ2RWDDg6h5k&branch=master)](https://travis-ci.org/wang502/gores) [![Go Report Card](https://goreportcard.com/badge/github.com/wang502/gores)](https://goreportcard.com/report/github.com/wang502/gores)
An asynchronous job execution system based on Redis

## Installation
Get the package
```
$ go get github.com/wang502/gores/gores
```
Import the package
```go
import "github.com/wang502/gores/gores"
```

## Usage
### Start local Redis server
```
$ git clone git@github.com:antirez/redis.git
$ cd redis
$ ./src/redis-server
```

### Configuration
Add a config.json in your project folder
```json
{
  "RedisURL": "127.0.0.1:6379",
  "RedisPassword": "mypassword",
  "BlpopMaxBlockTime" : 1,
  "MaxWorkers": 2,
  "Queues": ["queue1", "queue2"],
  "DispatcherTimeout": 5,
  "WorkerTimeout": 5
}
```
- ***RedisURL***: Redis server address. If you run in a local Redis, the dafault host is ```127.0.0.1:6379```
- ***RedisPassword***: Redis password. If the password is not set, then password can be any string.
- ***BlpopMaxBlockTime***: Blocking time when calling BLPOP command in Redis.
- ***MaxWorkers***: Maximum number of concurrent workers, each worker is a separate goroutine that execute specific task on the fetched item.
- ***Queues***: Array of queue names on Redis message broker.
- ***DispatcherTimeout***: Duration dispatcher will wait to dispatch new job before quitting.
- ***WorkerTimeout***: Duration worker will wait to process new job before quitting.

Initialize config
```go
configPath := flag.String("c", "config.json", "path to configuration file")
flag.Parse()
config, err := gores.InitConfig(*configPath)
```

### Enqueue job to Redis queue
A job is a Go map. It is required to have several keys:
- ***Name***: name of the item to enqueue, items with different names are mapped to different tasks.
- ***Queue***: name of the queue you want to put the item in.
- ***Args***: the required arguments that you need in order for the workers to execute those tasks.
- ***Enqueue_timestamp***: the Unix timestamp of when the item is enqueued.

```go
resq := gores.NewResQ(config)
job := map[string]interface{}{
  "Name": "Rectangle",
  "Queue": "TestJob",
  "Args": map[string]interface{}{
                "Length": 10,
                "Width": 10,
          },
  "Enqueue_timestamp": time.Now().Unix(),
}

err = resq.Enqueue(job)
if err != nil {
	log.Fatalf("ERROR Enqueue item to ResQ")
}
```

```
$ go run main.go -c ./config.json -o produce
```

### Define tasks
```go
package tasks

// task for item with 'Name' = 'Rectangle'
// calculating the area of an rectangle by multiplying Length with Width
func CalculateArea(args map[string]interface{}) error {
    var err error

    length := args["Length"]
    width := args["Width"]
    if length == nil || width == nil {
        err = errors.New("Map has no required attributes")
        return err
    }
    
    fmt.Printf("The area is %d\n", int(length.(float64)) * int(width.(float64)))
    return err
}
```

### Launch workers to consume items and execute tasks
```go
tasks := map[string]interface{}{
              "Item": tasks.PrintItem,
              "Rectangle": tasks.CalculateArea,
         }
gores.Launch(config, &tasks)
```

```
$ go run main.go -c ./config.json -o consume
```

The output will be:
```
The rectangle area is 100
```

### Info about processed/failed job
```go
resq := gores.NewResQ(config)
if resq == nil {
    log.Fatalf("resq is nil")
}

info, err := resq.Info()
if err != nil {
  log.Fatal(err)
}

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
```

The output will be:
```
Gores Info:
queues : 2
workers : 0
failed : 0
host : 127.0.0.1:6379
pending : 0
processed : 1
```

## Contribution
Please feel free to suggest new features. Also open to pull request!
