package gores

import (
    "fmt"
    "strconv"
    "testing"
)

var (
  config = &Config{
             REDISURL: "127.0.0.1:6379",
             REDIS_PW: "mypassword",
             BLPOP_MAX_BLOCK_TIME: 1,
             MAX_WORKERS: 2,
             Queues: []string{"TestJob", "TestScheduler"},
           }
    basic_sche = NewScheduler(config)
    resq = NewResQ(config)
    item = map[string]interface{}{
             "Name": "TestItem",
             "Queue": "TestScheduler",
             "Args": map[string]interface{}{
                          "id": 1,
                      },
             "Enqueue_timestamp": resq.CurrentTime(),
             "Retry": true,
             "Retry_every": 10,
           }
)

func TestNewScheduler(t *testing.T){
    sche := NewScheduler(config)
    if sche == nil {
        t.Errorf("ERROR initialize Scheduler")
    }
}

func TestHandleDelayedItems(t *testing.T){
    // enqueue item to delayed queue
    err := resq.Enqueue_at(1483079527, item)
    if err != nil {
        t.Errorf("ERROR Enqueue item at timestamp %d", 1483079527)
    }
    basic_sche.Run()

    delayed_queue_size := resq.SizeOfQueue(fmt.Sprintf(DEPLAYED_QUEUE_PREFIX, strconv.FormatInt(1483079527, 10)))
    if delayed_queue_size != 0 {
        t.Errorf("Scheduler worker did not handle delayed items")
    }

    queue_size := resq.Size(item["Queue"].(string))
    if queue_size != 1 {
        t.Errorf("Scheduler worker did not enqueue delayed item to resq:queue:%s", item["Queue"].(string))
    }

    resq.Pop(item["Queue"].(string))
}
