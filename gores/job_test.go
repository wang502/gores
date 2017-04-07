package gores

import (
	"errors"
	"fmt"
	"testing"
)

type MockJob struct {
	queue   string
	payload map[string]interface{}
}

func (job *MockJob) String() string {
	res := fmt.Sprintf("Job{%s}|%s", job.queue, job.payload["Name"])
	return res
}

func (job *MockJob) PerformTask(tasks *map[string]interface{}) error {
	structName := job.payload["Name"].(string)
	args := job.payload["Args"].(map[string]interface{})
	task := (*tasks)[structName]
	if task == nil {
		return errors.New("Task is not registered in tasks map")
	}
	// execute targeted task
	return task.(func(map[string]interface{}) error)(args)
}

func (job *MockJob) Retry(map[string]interface{}) bool {
	return true
}

func (job *MockJob) Failed()    {}
func (job *MockJob) Processed() {}

func TestNewJob(t *testing.T) {
	err := resq.push("TestItem", item)
	if err != nil {
		t.Errorf("Error Push to queue")
	}
	ret := resq.Pop("TestItem")
	job := NewJob("TestItem", ret, resq, "TestWorker")
	if job == nil {
		t.Errorf("NewJob() can not create new job")
	}
}

func TestPerform(t *testing.T) {
	job := &MockJob{"TestItem", item}
	err := job.PerformTask(&tasks)
	if err != nil {
		t.Errorf("Job Perform() ERROR")
	}
}
