package gores

import (
    "errors"
    "fmt"
    "os"
    "strings"
    "time"
    "github.com/deckarep/golang-set"
)

type Worker struct{
    id string
    queues mapset.Set
    shutdown bool
    child string
    pid int
    hostname string
    resq *ResQ
    started int64
}

func NewWorker(queues mapset.Set) *Worker {
    resq := NewResQ()
    if resq == nil {
        return nil
    }
    hostname, _ := os.Hostname()
    return &Worker{
              id : "",
              queues: queues,
              shutdown: false,
              child: "",
              pid: os.Getpid(),
              hostname: hostname,
              resq: resq,
              started: 0,
           }
}

func NewWorkerFromString(server string, password string, queues mapset.Set) *Worker{
    resq := NewResQFromString(server, password)
    if resq == nil {
        return nil
    }
    hostname, _ := os.Hostname()
    return &Worker{
              id : "",
              queues: queues,
              shutdown: false,
              child: "",
              pid: os.Getpid(),
              hostname: hostname,
              resq: resq,
              started: 0,
           }
}

// Worker ID
// hostname:pid:queue1,queue2,queue3
func (worker *Worker) String() string {
    qs := ""
    it := worker.queues.Iterator()
    for elem := range it.C {
        qs += elem.(string) + ","
    }
    return fmt.Sprintf("%s:%d:%s", worker.hostname, worker.pid, qs[:len(qs)-1])
}

func (worker *Worker) RegisterWorker() error{
    conn := worker.resq.pool.Get()
    _, err := conn.Do("SADD", WATCHED_WORKERS, worker.String())
    if err != nil {
        err = errors.New("ERROR Register Wroker")
    }
    worker.started = time.Now().Unix()
    return err
}

func (worker *Worker) UnregisterWorker() error {
    conn := worker.resq.pool.Get()
    _, err := conn.Do("SREM", WATCHED_WORKERS, worker.String())
    if err != nil {
        err = errors.New("ERROR Unregsiter Worker")
    }
    worker.started = 0

    p_stat := NewStat(fmt.Sprintf("processed:%s", worker.String()), worker.resq)
    p_stat.Clear()

    f_stat := NewStat(fmt.Sprintf("falied:%s", worker.String()), worker.resq)
    f_stat.Clear()

    return err
}

func (worker *Worker) PruneDeadWorkers() error {
    //all_workers := worker.All(worker.resq)
    return nil
}

func (worker *Worker) All(resq *ResQ) []*Worker {
    workers := resq.Workers()
    ret := make([]*Worker, len(workers))
    for i, w := range workers{
        ret[i] = worker.Find(w, resq)
    }
    return ret
}

func (worker *Worker) Find(worker_id string, resq *ResQ) *Worker {
    var new_worker *Worker
    if worker.Exists(worker_id) == 1 {
        id_tokens := strings.Split(worker_id, ":")
        q_slice := strings.Split(id_tokens[len(id_tokens)-1], ",")

        in_slice := make([]interface{}, len(q_slice))
        for i, q := range q_slice {
            in_slice[i] = q
        }
        q_set := mapset.NewSetFromSlice(in_slice)

        new_worker =  NewWorker(q_set)
        new_worker.id = worker_id
    }
    return new_worker
}

func (worker *Worker) Exists(worker_id string) int64 {
    reply, err := worker.resq.pool.Get().Do("SISMEMBER", WATCHED_WORKERS, worker_id)
    if err != nil || reply == nil {
        return 0
    }
    return reply.(int64)
}
