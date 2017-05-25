package gores

import (
	"os"
	"strings"
	"testing"

	"github.com/deckarep/golang-set"
)

var (
	queues = []interface{}{"TestItem", "Comment"}
	qSet   = mapset.NewSetFromSlice(queues)

	// config declared in gores_test.go
	worker = NewWorker(config, qSet, 1)
)

func TestNewWorker(t *testing.T) {
	if worker == nil {
		t.Errorf("NewWorker() ERROR")
	}
}

func TestWorkerString(t *testing.T) {
	workerID := worker.String()
	idTokens := strings.Split(workerID, ":")
	hostname, _ := os.Hostname()
	if strings.Compare(idTokens[0], hostname) != 0 {
		t.Errorf("Worker id contains incorrect hostname")
	}
	// host:pid:goroutine_id:queue1,queue2
	if strings.Compare(idTokens[3], "Comment,TestItem") != 0 && strings.Compare(idTokens[3], "TestItem,Comment") != 0 {
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

func MakeTestWorkers(num int) []*Worker {
	testQueue := []interface{}{"test1", "test2", "test3"}
	testQueueSet := mapset.NewSetFromSlice(testQueue)
	ret := make([]*Worker, num)
	for i := 0; i < num; i++ {
		ret[i] = NewWorker(config, testQueueSet, i+1)
	}
	return ret
}

func TestAll(t *testing.T) {
	testWorkers := MakeTestWorkers(1)
	for _, w := range testWorkers {
		err := w.RegisterWorker()
		if err != nil {
			t.Errorf("Error Register Worker")
		}
	}

	allWorkers := testWorkers[0].All(testWorkers[0].Gores())
	if len(allWorkers) != len(testWorkers) {
		t.Errorf("Worker All() did not return all workers")
	}

	for _, w := range testWorkers {
		err := w.UnregisterWorker()
		if err != nil {
			t.Errorf("ERROR Unregister Worker")
		}
	}

	allWorkers = testWorkers[0].All(testWorkers[0].Gores())
	if len(allWorkers) != 0 {
		t.Errorf("Worker Unregsiter Worker unsuccessful")
	}
}
