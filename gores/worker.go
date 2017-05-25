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
	gores       *Gores
	started     int64
	timeout     int
	jobChan     chan *Job
}

// NewWorker initlizes new worker
func NewWorker(config *Config, queues mapset.Set, goroutineID int) *Worker {
	gores := NewGores(config)
	if gores == nil {
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
		gores:       gores,
		started:     0,
		timeout:     config.WorkerTimeout,
		jobChan:     make(chan *Job),
	}
}

// Gores returns the pointer to embedded Gores object
func (worker *Worker) Gores() *Gores {
	/* export access to **gores** identifier to other package*/
	return worker.gores
}

// String returns the string representation of this worker
func (worker *Worker) String() string {
	/* Worker ID
	   hostname:pid:queue1,queue2,queue3
	*/
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
	conn := worker.gores.pool.Get()
	if conn == nil {
		return errors.New("RegisterWorker failed: Redis pool's connection is nil")
	}

	_, err := conn.Do("SADD", watchedWorkers, worker.String())
	if err != nil {
		return fmt.Errorf("RegisterWorker failed: %s", err)
	}
	worker.started = time.Now().Unix()
	return err
}

// UnregisterWorker delets all information related to this worker from Redis
func (worker *Worker) UnregisterWorker() error {
	conn := worker.gores.pool.Get()
	if conn == nil {
		return errors.New("UnregisterWorker failed: Redis pool's connection is nil")
	}

	_, err := conn.Do("SREM", watchedWorkers, worker.String())
	if err != nil {
		return fmt.Errorf("UnregisterWorker failed: %s", err)
	}
	worker.started = 0

	pStat := NewStat(fmt.Sprintf("processed:%s", worker.String()), worker.gores)
	pStat.Clear()

	fStat := NewStat(fmt.Sprintf("falied:%s", worker.String()), worker.gores)
	fStat.Clear()

	return nil
}

// PruneDeadWorkers delets the worker information
func (worker *Worker) PruneDeadWorkers() error {
	allWorkers := worker.All(worker.gores)
	allPids := worker.WorkerPids()
	for _, w := range allWorkers {
		idTokens := strings.Split(w.id, ":")
		host := idTokens[0]
		wPid := idTokens[1]
		if strings.Compare(host, worker.hostname) != 0 {
			continue
		}

		if allPids.Contains(wPid) {
			continue
		}

		fmt.Printf("Pruning dead worker: %s\n", w.String())
		if w != nil {
			if err := w.UnregisterWorker(); err != nil {
				return fmt.Errorf("PruneDeadWorkers failed: %s", err)
			}
		}
	}

	return nil
}

// All retruns a slice of existing workers
func (worker *Worker) All(gores *Gores) []*Worker {
	workerIDs := gores.Workers()
	allWorkers := make([]*Worker, len(workerIDs))
	for i, w := range workerIDs {
		allWorkers[i] = worker.Find(w, gores)
	}

	return allWorkers
}

// Find retruns the worker with given worker id
func (worker *Worker) Find(workerID string, gores *Gores) *Worker {
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

		config := worker.gores.config
		newWorker = NewWorker(config, qSet, goroutineID)
		newWorker.id = workerID
	}

	return newWorker
}

// Exists checks whether the worker with given id exists
func (worker *Worker) Exists(workerID string) int64 {
	reply, err := worker.gores.pool.Get().Do("SISMEMBER", watchedWorkers, workerID)
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
	return len(worker.gores.Workers())
}

// Start starts the worker and start working on tasks
func (worker *Worker) Start(dispatcher *Dispatcher, tasks *map[string]interface{}) error {
	err := worker.PruneDeadWorkers()
	if err != nil {
		return fmt.Errorf("startup failed: %s", err)
	}
	err = worker.RegisterWorker()
	if err != nil {
		return fmt.Errorf("startup failed: %s", err)
	}
	worker.work(dispatcher, tasks)

	err = worker.UnregisterWorker()
	if err != nil {
		return fmt.Errorf("startup failed: %s", err)
	}

	return nil
}

// work keeps fetching jobs from dispatcher and execute tasks until time out
func (worker *Worker) work(dispatcher *Dispatcher, tasks *map[string]interface{}) {
	for {
		select {
		case job, ok := <-worker.jobChan:
			if !ok {
				return
			}

			if err := ExecuteJob(job, tasks); err != nil {
				log.Printf("failed to execute job: %s", err)
			}
		}
	}
}
