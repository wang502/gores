package gores

import (
    "github.com/deckarep/golang-set"
)

type worker struct{
    queues mapset.Set
    shutdown bool
    child string
    resq *ResQ
}

func NewWorker(server string, password string, queues mapset.Set) *worker{
    resq := NewResQFromString(server, password)
    if resq == nil {
        return nil
    }
    return &worker{
              queues: queues,
              shutdown: false,
              child: "",
              resq: resq,
           }
}
