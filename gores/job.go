package gores

import (
    "errors"
    "fmt"
    "time"
    "github.com/deckarep/golang-set"
)

type Job struct {
    queue string
    payload map[string]interface{}
    resq *ResQ
    worker string
    enqueue_timestamp int64
}

func NewJob(queue string, payload map[string]interface{}, resq *ResQ, worker string) *Job {
    var timestamp int64
    _, ok := payload["Enqueue_timestamp"]
    if !ok {
        timestamp = 0
    } else {
        // Redis LPOP reply json, timestamp will be parsed to be float64
        // Any numbers from unmarshalled JSON will be float64 by default
        // So we first need to do a type conversion to float64
        timestamp = int64(payload["Enqueue_timestamp"].(float64))
    }
    return &Job{
                queue: queue,
                payload: payload,
                resq: resq,
                worker: worker,
                enqueue_timestamp: timestamp,
            }
}

func (job *Job) String() string {
    res := fmt.Sprintf("Job{%s}|%s", job.queue, job.payload["Name"])
    return res
}

func (job *Job) PerformTask(tasks *map[string]interface{}) error {
    struct_name := job.payload["Name"].(string)
    args := job.payload["Args"].(map[string]interface{})
    metadata := make(map[string]interface{})
    for k, v := range args {
        metadata[k] = v
    }
    if job.enqueue_timestamp != 0 {
        metadata["enqueue_timestamp"] = job.enqueue_timestamp
    }
    metadata["failed"] = false
    now := time.Now().Unix()
    metadata["perfomed_timestamp"] = now

    var err error
    task := (*tasks)[struct_name]
    if task == nil {
        err = errors.New("Task is not registered in tasks map")
        return err
    }
    // execute targeted task
    err = task.(func(map[string]interface{}) error)(args)
    if err != nil {
        metadata["failed"] = true
        if job.Retry(job.payload) {
            metadata["retried"] = true
        } else {
            metadata["retried"] = false
        }
        job.Failed()
        // deal with metadata
    }
    job.Processed()
    return err
}

func (job *Job) Retry(payload map[string]interface{}) bool {
    _, toRetry := job.payload["Retry"]
    retry_every := job.payload["Retry_every"]
    if !toRetry || retry_every == nil {
        return false
    } else {
        now := job.resq.CurrentTime()
        retry_at := now + int64(retry_every.(float64))
        //fmt.Printf("retry_at: %d\n", retry_at)
        err := job.resq.Enqueue_at(retry_at, payload)
        if err != nil {
            return false
        }
        return true
    }
}

func (job *Job) Failed(){
    NewStat("failed", job.resq).Incr()
    NewStat(fmt.Sprintf("failed:%s", job.String()), job.resq).Incr()
}

func (job *Job) Processed(){
    NewStat("processed", job.resq).Incr()
    NewStat(fmt.Sprintf("processed:%s", job.String()), job.resq).Incr()
}

func ReserveJob(resq *ResQ, queues mapset.Set, worker_id string) *Job {
    queue, payload := resq.BlockPop(queues)
    if payload != nil {
        return NewJob(queue, payload, resq, worker_id)
    }
    return nil
}
