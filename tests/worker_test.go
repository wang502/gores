package tests

import (
    "os"
    "strings"
    "testing"
    "github.com/deckarep/golang-set"
    "github.com/wang502/gores/gores"
)

var (
    queues = []interface{}{"TestItem", "Comment"}
    q_set = mapset.NewSetFromSlice(queues)
    worker = gores.NewWorker(q_set, 1)
)

func TestNewWorker(t *testing.T){
    if worker == nil {
        t.Errorf("NewWorker() ERROR")
    }
}

func TestWorkerString(t *testing.T){
    worker_id := worker.String()
    id_tokens := strings.Split(worker_id, ":")
    hostname, _ := os.Hostname()
    if strings.Compare(id_tokens[0], hostname) != 0 {
        t.Errorf("Worker id contains incorrect hostname")
    }
    // host:pid:goroutine_id:queue1,queue2
    if strings.Compare(id_tokens[3], "Comment,TestItem") != 0 && strings.Compare(id_tokens[3], "TestItem,Comment") != 0 {
        t.Errorf("Worker contains incorrect queue name")
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

func MakeTestWorkers(num int) []*gores.Worker {
    test_queue := []interface{}{"test1", "test2", "test3"}
    test_queue_set := mapset.NewSetFromSlice(test_queue)
    ret := make([]*gores.Worker, num)
    for i:=0; i<num; i++ {
        ret[i] = gores.NewWorker(test_queue_set, i+1)
    }
    return ret
}

func TestAll(t *testing.T) {
    test_workers := MakeTestWorkers(1)
    for _, w := range test_workers {
        err := w.RegisterWorker()
        if err != nil {
            t.Errorf("Error Register Worker")
        }
    }

    all_workers := test_workers[0].All(test_workers[0].ResQ())
    if len(all_workers) != len(test_workers) {
        t.Errorf("Worker All() did not return all workers")
    }

    for _, w := range test_workers {
        err := w.UnregisterWorker()
        if err != nil {
            t.Errorf("ERROR Unregister Worker")
        }
    }

    all_workers = test_workers[0].All(test_workers[0].ResQ())
    if len(all_workers) != 0 {
        t.Errorf("Worker Unregsiter Worker unsuccessful")
    }
}
