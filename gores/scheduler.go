package gores

import (
    "log"
)

type Scheduler struct {
    resq *ResQ
    shutdown bool
    timestamp_chan chan int64
}

func NewScheduler() *Scheduler {
    return &Scheduler{
              resq: NewResQ(),
              shutdown: false,
              timestamp_chan: make(chan int64, 1),
           }
}

func (sche *Scheduler) ScheduleShutdown(){
    sche.shutdown = true
}

func (sche *Scheduler) NextDelayedTimestamps(){
    for {
        timestamp := sche.resq.NextDelayedTimestamp()
        if timestamp != 0 {
            sche.timestamp_chan <- timestamp
        } else {
            break
        }
    }
}

func (sche *Scheduler) HandleDelayedItems(){
    for {
        if sche.shutdown {
            break
        }
        select{
        case timestamp := <- sche.timestamp_chan:
            item := sche.resq.NextItemForTimestamp(timestamp)
            err := sche.resq.Enqueue_at(timestamp, item)
            if err != nil {
                log.Fatalf("ERROR Enqueue Delayed Item")
            }
        }
    }
}

func (sche *Scheduler) Run() {
    sche.HandleDelayedItems()
}
