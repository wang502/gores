package gores

import (
	"fmt"
	"time"

	"github.com/deckarep/golang-set"
)

// Job represents a job that needs to be executed
type Job struct {
	queue            string
	payload          map[string]interface{}
	resq             *ResQ
	enqueueTimestamp int64
}

// NewJob initilizes a new job object
func NewJob(queue string, payload map[string]interface{}, resq *ResQ) *Job {
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
		queue:            queue,
		payload:          payload,
		resq:             resq,
		enqueueTimestamp: timestamp,
	}
}

// String returns the string representation of the job object
func (job *Job) String() string {
	res := fmt.Sprintf("Job{%s}|%s", job.queue, job.payload["Name"])
	return res
}

// Payload returns the payload map inside the job struct
func (job *Job) Payload() map[string]interface{} {
	return job.payload
}

// Retry enqueues the failed job back to Redis queue
func (job *Job) Retry(payload map[string]interface{}) bool {
	_, ok1 := job.payload["Retry"]
	retryEvery, ok2 := job.payload["Retry_every"]
	if !ok1 || !ok2 {
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
func (job *Job) Failed() {
	NewStat("failed", job.resq).Incr()
	NewStat(fmt.Sprintf("failed:%s", job.String()), job.resq).Incr()
}

// Processed updates the state of job to be processed
func (job *Job) Processed() {
	if job.resq == nil {
		return
	}
	NewStat("processed", job.resq).Incr()
	NewStat(fmt.Sprintf("processed:%s", job.String()), job.resq).Incr()
}

// ReserveJob uses BLPOP command to fetch job from Redis
func ReserveJob(resq *ResQ, queues mapset.Set) (*Job, error) {
	queue, payload, err := resq.BlockPop(queues)
	if err != nil {
		return nil, fmt.Errorf("reserve job failed: %s", err)
	}
	return NewJob(queue, payload, resq), nil
}

// ExecuteJob executes the job, given the mapper of corresponding worker
func ExecuteJob(job *Job, tasks *map[string]interface{}) error {
	// check whether payload is valid
	jobName, ok1 := job.payload["Name"]
	jobArgs, ok2 := job.payload["Args"]
	if !ok1 || !ok2 {
		return fmt.Errorf("execute job failed: job payload has no key %s or %s", "Name", "Args")
	}

	name, ok1 := jobName.(string)
	args, ok2 := jobArgs.(map[string]interface{})
	if !ok1 || !ok2 {
		return fmt.Errorf("execute job failed: job args is not a map or job name is not a string")
	}

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

	task := (*tasks)[name]
	if task == nil {
		return fmt.Errorf("execute task failed: task with name %s is not registered in tasks map", jobName)
	}
	// execute targeted task
	err := task.(func(map[string]interface{}) error)(args)
	if err != nil {
		metadata["failed"] = true
		if job.Retry(job.payload) {
			metadata["retried"] = true
		} else {
			metadata["retried"] = false
		}
		job.Failed()
		// deal with metadata here
		return fmt.Errorf("execute job failed: %s", err)
	}
	job.Processed()
	return nil
}
