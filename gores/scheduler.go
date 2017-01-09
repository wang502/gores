package gores

import (
    "log"
)

type Scheduler struct {
    resq *ResQ
    timestampChan chan int64
    shutdownChan chan bool
}

func NewScheduler(config *Config) *Scheduler {
    var sche *Scheduler
    resq := NewResQ(config)
    if resq == nil {
        log.Fatalf("ERROR Initializing ResQ(), cannot initialize Scheduler")
        return nil
    }
    sche = &Scheduler{
              resq: resq,
              timestampChan: make(chan int64, 1),
              shutdownChan: make(chan bool, 1),
           }
    return sche
}

func (sche *Scheduler) ScheduleShutdown(){
    sche.shutdownChan <- true
}

func (sche *Scheduler) NextDelayedTimestamps(){
    for {
        timestamp := sche.resq.NextDelayedTimestamp()
        log.Printf("timestamp of delayed item: %d\n", timestamp)
        if timestamp != 0 {
            sche.timestampChan <- timestamp
        } else {
            /* breaks when there is no delayed items in the 'resq:delayed:timestamp' queue*/
            break
        }
    }
    sche.ScheduleShutdown()
}

func (sche *Scheduler) HandleDelayedItems(){
    go sche.NextDelayedTimestamps()
    for {
        select{
        case timestamp := <- sche.timestampChan:
            item := sche.resq.NextItemForTimestamp(timestamp)
            if item != nil {
                log.Println(item)
                err := sche.resq.Enqueue(item)
                if err != nil {
                  log.Fatalf("ERROR Enqueue Delayed Item: %s", err)
                }
            }
        case <-sche.shutdownChan:
            return
        }
    }
    log.Println("Finish Handling Delayed Items")
}

func (sche *Scheduler) Run() {
    sche.HandleDelayedItems()
}
