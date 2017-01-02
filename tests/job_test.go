package tests

import (
    "testing"
    "github.com/wang502/gores/gores"
)

func TestNewJob(t *testing.T) {
    err := resq.Push("TestItem", item)
    if err != nil {
        t.Errorf("Error Push to queue")
    }
    ret := resq.Pop("TestItem")
    job := gores.NewJob("TestItem", ret, resq, "TestWorker")
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
  job := gores.NewJob("TestItem", ret, resq, "TestWorker")
  err = job.Perform()
  if err != nil {
      t.Errorf("Job Perform() ERROR")
  }
}
