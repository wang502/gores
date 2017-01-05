package gores

import (
    "testing"
)

func TestNewJob(t *testing.T) {
    err := resq.Push("TestItem", item)
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
  err := resq.Push("TestItem", item)
  if err != nil {
      t.Errorf("Error Push to queue")
  }
  ret := resq.Pop("TestItem")
  job := NewJob("TestItem", ret, resq, "TestWorker")
  err = job.PerformTask(&tasks)
  if err != nil {
      t.Errorf("Job Perform() ERROR")
  }
}
