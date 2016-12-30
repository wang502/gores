package gores

import (
    "fmt"
    "sync"
    _ "time"
    "github.com/deckarep/golang-set"
)

var worker_ids_channel chan string

type Dispatcher struct {
    resq *ResQ
    max_workers int
    job_channel chan *Job
    done_channel chan int
    queues mapset.Set
}

func NewDispatcher(resq *ResQ, max_workers int, queues mapset.Set) *Dispatcher{
    worker_ids_channel = make(chan string, max_workers)
    return &Dispatcher{
              resq: resq,
              max_workers: max_workers,
              job_channel: make(chan *Job, max_workers),
              queues: queues,
            }
}

func (disp *Dispatcher) Run(){
    var wg sync.WaitGroup
    for i:=0; i<disp.max_workers; i++{
        worker := NewWorker(disp.queues)
        worker_id := worker.String()
        fmt.Println(worker_id)
        worker_ids_channel <- worker_id
        fmt.Println(len(worker_ids_channel))

        wg.Add(1)
        go worker.Startup(disp, &wg)
    }
    wg.Add(1)
    go disp.Dispatch(&wg)
    wg.Wait()
}

func (disp *Dispatcher) Dispatch(wg *sync.WaitGroup){
    for {
        select {
        case worker_id := <-worker_ids_channel:
            go func(worker_id string){
              for {
                fmt.Printf("work id: %s\n", worker_id)
                job := ReserveJob(disp.resq, disp.queues, worker_id)
                if job != nil {
                  fmt.Printf("job: %s\n", job.String())
                  disp.job_channel<-job
                }
              }
            }(worker_id)
        }
    }
    wg.Done()
}
