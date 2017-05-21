package gores

import (
	"testing"
)

func TestNewJob(t *testing.T) {
	err := resq.push("TestItem", item)
	if err != nil {
		t.Errorf("Error Push to queue")
	}
	ret, err := resq.Pop("TestItem")
	if err != nil {
		t.Errorf("%s", err)
	}
	job := NewJob("TestItem", ret, resq)
	if job == nil {
		t.Errorf("NewJob() can not create new job")
	}
}

func TestExecuteJob(t *testing.T) {
	mockJob := &Job{
		queue:            "TestItem",
		payload:          item,
		resq:             nil,
		enqueueTimestamp: 0}

	err := ExecuteJob(mockJob, &tasks)
	if err != nil {
		t.Errorf("Job Perform() ERROR: %s", err)
	}
}
