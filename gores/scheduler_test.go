package gores

import (
	"fmt"
	"strconv"
	"testing"
)

var (
	config = &Config{
		RedisURL:          "127.0.0.1:6379",
		RedisPassword:     "mypassword",
		BlpopMaxBlockTime: 1,
		MaxWorkers:        2,
		Queues:            []string{"TestJob", "TestScheduler"},
	}

	basicSche = NewScheduler(config)
	gores     = NewGores(config)
	item      = map[string]interface{}{
		"Name":  "TestItem",
		"Queue": "TestScheduler",
		"Args": map[string]interface{}{
			"id": 1,
		},
		"Enqueue_timestamp": gores.CurrentTime(),
		"Retry":             true,
		"Retry_every":       10,
	}
)

func TestNewScheduler(t *testing.T) {
	sche := NewScheduler(config)
	if sche == nil {
		t.Errorf("ERROR initialize Scheduler")
	}
}

func TestHandleDelayedItems(t *testing.T) {
	// enqueue item to delayed queue
	err := gores.EnqueueAt(1483079527, item)
	if err != nil {
		t.Errorf("ERROR Enqueue item at timestamp %d", 1483079527)
	}
	basicSche.Run()

	delayedQueueSize := gores.SizeOfQueue(fmt.Sprintf(delayedQueuePrefix, strconv.FormatInt(1483079527, 10)))
	if delayedQueueSize != 0 {
		t.Errorf("Scheduler worker did not handle delayed items")
	}

	queueSize, err := gores.Size(item["Queue"].(string))
	if err != nil {
		t.Errorf("%s", err)
	}
	if queueSize != 1 {
		t.Errorf("Scheduler worker did not enqueue delayed item to gores:queue:%s", item["Queue"].(string))
	}

	gores.Pop(item["Queue"].(string))
}
