package tests

import (
    "testing"
    "github.com/wang502/gores/gores"
)

var (resq = gores.NewResQ()
     args = map[string]interface{}{"id": 1}
     item = gores.TestItem{
              Name: "TestItem",
              Queue: "TestItem",
              Args: args,
              Enqueue_timestamp: resq.CurrentTime(),
              Retry: true,
              Retry_every: 10,
            }
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
