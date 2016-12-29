package tests

import (
    "strings"
    "testing"
    "github.com/deckarep/golang-set"
    "github.com/wang502/gores/gores"
)

var (
    queues = []interface{}{"TestItem", "Comment"}
    q_set = mapset.NewSetFromSlice(queues)
    worker = gores.NewWorker(q_set)
)

func TestNewWorker(t *testing.T){
    if worker == nil {
        t.Errorf("NewWorker() ERROR")
    }
}

func TestWorkerString(t *testing.T){
    worker_id := worker.String()
    id_tokens := strings.Split(worker_id, ":")
    if strings.Compare(id_tokens[0], "xingxing.local") != 0 {
        t.Errorf("Worker id contains incorrect hostname")
    }
    if strings.Compare(id_tokens[2], "Comment,TestItem") != 0 && strings.Compare(id_tokens[2], "TestItem,Comment") != 0 {
        t.Errorf("Worker if contains incorrect queue name")
    }
}

func TestRegisterWorker(t *testing.T) {
    err := worker.RegisterWorker()
    if err != nil {
        t.Error("Error Register Worker")
    }

    exist := worker.Exists(worker.String())
    if exist != 1 {
        t.Errorf("Error Register Worker")
    }
}

func TestUnregisterWorker(t *testing.T) {
    err := worker.UnregisterWorker()
    if err != nil {
        t.Errorf("Error Unregister Worker")
    }
}
