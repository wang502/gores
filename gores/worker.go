package gores

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/deckarep/golang-set"
)

// Worker represents a object involved in Gores
type Worker struct {
	id          string
	goroutineID int
	queues      mapset.Set
	shutdown    bool
	child       string
	pid         int
	hostname    string
	resq        *ResQ
	started     int64
	timeout     int
}

// NewWorker initlizes new worker
func NewWorker(config *Config, queues mapset.Set, goroutineID int) *Worker {
	resq := NewResQ(config)
	if resq == nil {
		return nil
	}
	hostname, _ := os.Hostname()
	return &Worker{
		id:          "",
		goroutineID: goroutineID,
		queues:      queues,
		shutdown:    false,
		child:       "",
		pid:         os.Getpid(),
		hostname:    hostname,
		resq:        resq,
		started:     0,
		timeout:     config.WorkerTimeout,
	}
}

// NewWorkerFromString initlizes new worker
func NewWorkerFromString(config *Config, server string, password string, queues mapset.Set, goroutineID int) *Worker {
	resq := NewResQFromString(config, server, password)
	if resq == nil {
		return nil
	}
	hostname, _ := os.Hostname()
	return &Worker{
		id:          "",
		goroutineID: goroutineID,
		queues:      queues,
		shutdown:    false,
		child:       "",
		pid:         os.Getpid(),
		hostname:    hostname,
		resq:        resq,
		started:     0,
	}
}

// ResQ returns the pointer to embeded ResQ object
func (worker *Worker) ResQ() *ResQ {
	/* export access to **resq** identifier to other package*/
	return worker.resq
}

// String returns the string representation of this worker
func (worker *Worker) String() string {
	/* Worker ID
	   hostname:pid:queue1,queue2,queue3 */
	if worker.id != "" {
		return worker.id
	}
	qs := ""
	it := worker.queues.Iterator()
	for elem := range it.C {
		qs += elem.(string) + ","
	}
	worker.id = fmt.Sprintf("%s:%d:%d:%s", worker.hostname, worker.pid, worker.goroutineID, qs[:len(qs)-1])
	return worker.id
}

// RegisterWorker saves information about this worker on Redis
func (worker *Worker) RegisterWorker() error {
	conn := worker.resq.pool.Get()
	if conn == nil {
		return errors.New("Redis pool's connection is nil")
	}

	_, err := conn.Do("SADD", WATCHED_WORKERS, worker.String())
	if err != nil {
		err = errors.New("ERROR Register Wroker")
	}
	worker.started = time.Now().Unix()
	return err
}

// UnregisterWorker delets all information related to this worker from Redis
func (worker *Worker) UnregisterWorker() error {
	conn := worker.resq.pool.Get()
	if conn == nil {
		return errors.New("Redis pool's connection is nil")
	}

	_, err := conn.Do("SREM", WATCHED_WORKERS, worker.String())
	if err != nil {
		err = errors.New("ERROR Unregsiter Worker")
	}
	worker.started = 0

	pStat := NewStat(fmt.Sprintf("processed:%s", worker.String()), worker.resq)
	pStat.Clear()

	fStat := NewStat(fmt.Sprintf("falied:%s", worker.String()), worker.resq)
	fStat.Clear()

	return err
}

// PruneDeadWorkers delets the worker information
func (this *Worker) PruneDeadWorkers() error {
	allWorkers := this.All(this.resq)
	allPids := this.WorkerPids()
	for _, w := range allWorkers {
		idTokens := strings.Split(w.id, ":")
		host := idTokens[0]
		wPid := idTokens[1]
		if strings.Compare(host, this.hostname) != 0 {
			continue
		}
		if allPids.Contains(wPid) {
			continue
		}
		fmt.Printf("Pruning dead worker: %s\n", w.String())
		if w != nil {
			w.UnregisterWorker()
		}
	}
	return nil
}

// All retruns a slice of existing workers
func (worker *Worker) All(resq *ResQ) []*Worker {
	workerIDs := resq.Workers()
	allWorkers := make([]*Worker, len(workerIDs))
	for i, w := range workerIDs {
		allWorkers[i] = worker.Find(w, resq)
	}
	return allWorkers
}

// Find retruns the worker with given worker id
func (worker *Worker) Find(workerID string, resq *ResQ) *Worker {
	var newWorker *Worker
	if worker.Exists(workerID) == 1 {
		idTokens := strings.Split(workerID, ":")
		goroutineID, _ := strconv.Atoi(idTokens[2])

		qSlice := strings.Split(idTokens[len(idTokens)-1], ",")
		inSlice := make([]interface{}, len(qSlice))
		for i, q := range qSlice {
			inSlice[i] = q
		}
		qSet := mapset.NewSetFromSlice(inSlice)

		config := worker.resq.config
		newWorker = NewWorker(config, qSet, goroutineID)
		newWorker.id = workerID
	}
	return newWorker
}

// Exists checks whether the worker with given id exists
func (worker *Worker) Exists(workerID string) int64 {
	reply, err := worker.resq.pool.Get().Do("SISMEMBER", WATCHED_WORKERS, workerID)
	if err != nil || reply == nil {
		return 0
	}
	return reply.(int64)
}

// WorkerPids returns a set of existing workers' ids
func (worker *Worker) WorkerPids() mapset.Set {
	/* Returns a set of all pids (as strings) on
	   this machine.  Used when pruning dead workers. */
	out, err := exec.Command("ps").Output()
	if err != nil {
		log.Fatal(err)
	}
	outLines := strings.Split(strings.TrimSpace(string(out)), "\n")
	inSlice := make([]interface{}, len(outLines)-1) // skip first row
	for i, line := range outLines[1:] {
		inSlice[i] = strings.Split(strings.TrimSpace(line), " ")[0] // pid at index 0
	}
	return mapset.NewSetFromSlice(inSlice)
}

// Size returns the total number of live workers
func (worker *Worker) Size() int {
	/* Return total number of workers */
	return len(worker.resq.Workers())
}

// Startup wakes up the worker and start working on tasks
func (worker *Worker) Startup(dispatcher *Dispatcher, tasks *map[string]interface{}) error {
	err := worker.PruneDeadWorkers()
	if err != nil {
		err = errors.New("Satrtup() ERROR when PruneDeadWorkers()")
		return err
	}
	err = worker.RegisterWorker()
	if err != nil {
		err = errors.New("Startup() ERROR when RegisterWorker()")
		return err
	}
	worker.work(dispatcher, tasks)

	err = worker.UnregisterWorker()
	return err
}

// work keeps fetching jobs from dispatcher and execute tasks until time out
func (worker *Worker) work(dispatcher *Dispatcher, tasks *map[string]interface{}) {
	for {
		select {
		case job := <-dispatcher.jobChannel:
			if err := job.PerformTask(tasks); err != nil {
				log.Fatalf("ERROR Perform Job, %s", err)
			}
		case <-time.After(time.Second * time.Duration(worker.timeout)):
			log.Printf("Timeout: worker | %s\n", worker.String())
			return
		}
	}
}
