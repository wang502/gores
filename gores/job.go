package gores

import (
	"errors"
	"fmt"
	"time"

	"github.com/deckarep/golang-set"
)

// Job is the interface that represents a job involved in Gores
type Job interface {
	String() string
	PerformTask(*map[string]interface{}) error
	Retry(map[string]interface{}) bool
	Failed()
	Processed()
}

// job represents a job that needs to be executed
type job struct {
	queue            string
	payload          map[string]interface{}
	resq             *ResQ
	worker           string
	enqueueTimestamp int64
}

// NewJob initilizes a new job object
func NewJob(queue string, payload map[string]interface{}, resq *ResQ, worker string) Job {
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
	return &job{
		queue:            queue,
		payload:          payload,
		resq:             resq,
		worker:           worker,
		enqueueTimestamp: timestamp,
	}
}

// String returns the string representation of the job object
func (job *job) String() string {
	res := fmt.Sprintf("Job{%s}|%s", job.queue, job.payload["Name"])
	return res
}

// PerformTask executes the job, given the mapper of corresponsing worker
func (job *job) PerformTask(tasks *map[string]interface{}) error {
	structName := job.payload["Name"].(string)
	args := job.payload["Args"].(map[string]interface{})
	metadata := make(map[string]interface{})
	for k, v := range args {
		metadata[k] = v
	}
	if job.enqueueTimestamp != 0 {
		metadata["enqueue_timestamp"] = job.enqueueTimestamp
	}
	metadata["failed"] = false
	now := time.Now().Unix()
	metadata["perfomed_timestamp"] = now

	var err error
	task := (*tasks)[structName]
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

// Retry enqueues the failed job back to Redis queue
func (job *job) Retry(payload map[string]interface{}) bool {
	_, toRetry := job.payload["Retry"]
	retryEvery := job.payload["Retry_every"]
	if !toRetry || retryEvery == nil {
		return false
	}

	now := job.resq.CurrentTime()
	retryAt := now + int64(retryEvery.(float64))
	//log.Printf("retry_at: %d\n", retry_at)
	err := job.resq.EnqueueAt(retryAt, payload)
	if err != nil {
		return false
	}
	return true
}

// Failed update the state of the job to be failed
func (job *job) Failed() {
	NewStat("failed", job.resq).Incr()
	NewStat(fmt.Sprintf("failed:%s", job.String()), job.resq).Incr()
}

// Processed updates the state of job to be processed
func (job *job) Processed() {
	NewStat("processed", job.resq).Incr()
	NewStat(fmt.Sprintf("processed:%s", job.String()), job.resq).Incr()
}

// ReserveJob uses BLPOP command to fetch job from Redis
func ReserveJob(resq *ResQ, queues mapset.Set, workerID string) Job {
	queue, payload := resq.BlockPop(queues)
	if payload != nil {
		return NewJob(queue, payload, resq, workerID)
	}
	return nil
}
