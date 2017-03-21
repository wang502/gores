package gores

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/deckarep/golang-set"
)

var workerIDChan chan string

// Dispatcher represents the dispatcher between Redis server and workers
type Dispatcher struct {
	resq        *ResQ
	maxWorkers  int
	jobChannel  chan *Job
	doneChannel chan int
	queues      mapset.Set
	timeout     int
}

// NewDispatcher creates Dispatcher instance
func NewDispatcher(resq *ResQ, config *Config, queues mapset.Set) *Dispatcher {
	if resq == nil || config.MAX_WORKERS <= 0 {
		log.Println("Invalid number of workers to initialize Dispatcher")
		return nil
	}
	workerIDChan = make(chan string, config.MAX_WORKERS)
	return &Dispatcher{
		resq:       resq,
		maxWorkers: config.MAX_WORKERS,
		jobChannel: make(chan *Job, config.MAX_WORKERS),
		queues:     queues,
		timeout:    config.DispatcherTimeout,
	}
}

// Run startups the Dispatcher
func (disp *Dispatcher) Run(tasks *map[string]interface{}) error {
	var wg sync.WaitGroup
	config := disp.resq.config

	for i := 0; i < disp.maxWorkers; i++ {
		worker := NewWorker(config, disp.queues, i+1)
		if worker == nil {
			return errors.New("ERROR running worker: worker is nil")
		}
		workerID := worker.String()
		workerIDChan <- workerID

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := worker.Startup(disp, tasks)
			if err != nil {
				log.Fatalf("ERROR startup worker: %s", err)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		disp.Dispatch()
	}()
	wg.Wait()
	return nil
}

// Dispatch lets Dispatcher transport jobs between Redis and Gores workers
func (disp *Dispatcher) Dispatch() {
	for {
		select {
		case workerID := <-workerIDChan:
			go func(workerID string) {
				for {
					job := ReserveJob(disp.resq, disp.queues, workerID)
					if job != nil {
						disp.jobChannel <- job
					}
				}
			}(workerID)
		case <-time.After(time.Second * time.Duration(disp.timeout)):
			log.Println("Timeout: Dispatch")
			break
		}
		break
	}
}
