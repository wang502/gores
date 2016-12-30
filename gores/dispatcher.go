package gores

import (
    "github.com/deckarep/golang-set"
)

var worker_ids_channel chan string

type Dispatcher struct {
    resq *ResQ
    max_workers int
    job_channel chan *Job
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
    for i:=0; i<disp.max_workers; i++{
        worker := NewWorker(disp.queues)
        worker_ids_channel <- worker.String()
        worker.Startup(disp)
    }

    go disp.Dispatch()
}

func (disp *Dispatcher) Dispatch(){
    for {
        select {
        case worker_id := <- worker_ids_channel:
            go func(worker_id string){
                job := ReserveJob(disp.resq, disp.queues, worker_id)
                disp.job_channel <- job
            }(worker_id)
        }
    }
}
