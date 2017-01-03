package gores

import (
    "errors"
    "fmt"
    "log"
    "os"
    "os/exec"
    "strings"
    "strconv"
    "sync"
    "time"
    "github.com/deckarep/golang-set"
)

type Worker struct{
    id string
    goroutine_id int
    queues mapset.Set
    shutdown bool
    child string
    pid int
    hostname string
    resq *ResQ
    started int64
}

func NewWorker(config *Config, queues mapset.Set, goroutine_id int) *Worker {
    resq := NewResQ(config)
    if resq == nil {
        return nil
    }
    hostname, _ := os.Hostname()
    return &Worker{
              id : "",
              goroutine_id : goroutine_id,
              queues: queues,
              shutdown: false,
              child: "",
              pid: os.Getpid(),
              hostname: hostname,
              resq: resq,
              started: 0,
           }
}

func NewWorkerFromString(config *Config, server string, password string, queues mapset.Set, goroutine_id int) *Worker{
    resq := NewResQFromString(config, server, password)
    if resq == nil {
        return nil
    }
    hostname, _ := os.Hostname()
    return &Worker{
              id : "",
              goroutine_id : goroutine_id,
              queues: queues,
              shutdown: false,
              child: "",
              pid: os.Getpid(),
              hostname: hostname,
              resq: resq,
              started: 0,
           }
}

func (worker *Worker) ResQ() *ResQ {
    /* export access to **resq** identifier to other package*/
    return worker.resq
}

func (worker *Worker) String() string {
    /* Worker ID
      hostname:pid:queue1,queue2,queue3 */
    if worker.id != "" {
        return worker.id
    } else {
        qs := ""
        it := worker.queues.Iterator()
        for elem := range it.C {
            qs += elem.(string) + ","
        }
        worker.id = fmt.Sprintf("%s:%d:%d:%s", worker.hostname, worker.pid, worker.goroutine_id, qs[:len(qs)-1])
        return worker.id
    }
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

func (this *Worker) PruneDeadWorkers() error {
    all_workers := this.All(this.resq)
    all_machine_pids := this.WorkerPids()
    for _, w := range all_workers {
        id_tokens := strings.Split(w.id, ":")
        host := id_tokens[0]
        w_pid := id_tokens[1]
        if strings.Compare(host, this.hostname) != 0 {
            continue
        }
        if all_machine_pids.Contains(w_pid) {
            continue
        }
        fmt.Printf("Pruning dead worker: %s\n", w.String())
        if w != nil {
          w.UnregisterWorker()
        }
    }
    return nil
}

func (worker *Worker) All(resq *ResQ) []*Worker {
    worker_ids := resq.Workers()
    all_workers := make([]*Worker, len(worker_ids))
    for i, w := range worker_ids{
        all_workers[i] = worker.Find(w, resq)
    }
    return all_workers
}

func (worker *Worker) Find(worker_id string, resq *ResQ) *Worker {
    var new_worker *Worker
    if worker.Exists(worker_id) == 1 {
        id_tokens := strings.Split(worker_id, ":")
        goroutine_id, _ := strconv.Atoi(id_tokens[2])

        q_slice := strings.Split(id_tokens[len(id_tokens)-1], ",")
        in_slice := make([]interface{}, len(q_slice))
        for i, q := range q_slice {
            in_slice[i] = q
        }
        q_set := mapset.NewSetFromSlice(in_slice)

        config := worker.resq.config
        new_worker =  NewWorker(config, q_set, goroutine_id)
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


func (worker *Worker) WorkerPids() mapset.Set{
    /* Returns a set of all pids (as strings) on
      this machine.  Used when pruning dead workers. */
    out, err := exec.Command("ps").Output()
    if err != nil {
        log.Fatal(err)
    }
    out_lines := strings.Split(strings.TrimSpace(string(out)), "\n")
    in_slice := make([]interface{}, len(out_lines)-1) // skip first row
    for i, line := range out_lines[1:] {
        in_slice[i] = strings.Split(strings.TrimSpace(line), " ")[0] // pid at index 0
    }
    return mapset.NewSetFromSlice(in_slice)
}

func (worker *Worker) Size() int {
    /* Return total number of workers */
    return len(worker.resq.Workers())
}

func (worker *Worker) Startup(dispatcher *Dispatcher, wg *sync.WaitGroup) error {
    err := worker.PruneDeadWorkers()
    if err != nil {
        err = errors.New("Satrtup() ERROR when PruneDeadWorkers()")
    }
    err = worker.RegisterWorker()
    if err != nil {
        err = errors.New("Startup() ERROR when RegisterWorker()")
    }
    worker.Work(dispatcher)

    wg.Done()
    return err
}

func (worker *Worker) Work(dispatcher *Dispatcher){
    for {
        select {
        case job := <-dispatcher.job_channel:
            if err := job.Perform(); err != nil {
                log.Fatalf("ERROR Perform Job, %s", err)
            }
        }
    }
}
