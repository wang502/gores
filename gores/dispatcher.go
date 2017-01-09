package gores

import (
    "errors"
    "log"
    "sync"
    "github.com/deckarep/golang-set"
)

var workerIdChan chan string

type Dispatcher struct {
    resq *ResQ
    maxWorkers int
    jobChannel chan *Job
    doneChannel chan int
    queues mapset.Set
}

func NewDispatcher(resq *ResQ, maxWorkers int, queues mapset.Set) *Dispatcher{
    if resq == nil || maxWorkers <= 0 {
        log.Println("Invalid arguments for initializing Dispatcher")
        return nil
    }
    workerIdChan = make(chan string, maxWorkers)
    return &Dispatcher{
              resq: resq,
              maxWorkers: maxWorkers,
              jobChannel: make(chan *Job, maxWorkers),
              queues: queues,
            }
}

func (disp *Dispatcher) Run(tasks *map[string]interface{}) error {
    var wg sync.WaitGroup
    config := disp.resq.config

    for i:=0; i<disp.maxWorkers; i++{
        worker := NewWorker(config, disp.queues, i+1)
        if worker == nil {
            return errors.New("ERROR running worker: worker is nil")
        }
        workerId := worker.String()
        workerIdChan <- workerId

        wg.Add(1)
        go worker.Startup(disp, &wg, tasks)
    }
    wg.Add(1)
    go disp.Dispatch(&wg)
    wg.Wait()
    return nil
}

func (disp *Dispatcher) Dispatch(wg *sync.WaitGroup){
    for {
        select {
        case workerId := <-workerIdChan:
            go func(workerId string){
              for {
                job := ReserveJob(disp.resq, disp.queues, workerId)
                if job != nil {
                  disp.jobChannel<-job
                }
              }
            }(workerId)
        }
    }
    wg.Done()
}
