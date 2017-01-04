# Gores
An asynchronous job execution system with Redis as message broker

## Installation
```
go get github.com/wang502/gores/gores
```

## Usage
### Configuration
Add a config.json in your project folder
```
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
```
import "github.com/wang502/gores/gores"
import log

resq := gores.NewResQ(config)
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
```

### Fire up workers
```

```
