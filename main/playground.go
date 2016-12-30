package main

import (
    "fmt"
    "time"
    "github.com/wang502/gores/gores"
    "github.com/deckarep/golang-set"
)
/*
type TestItem struct{
    Name string `json:"Name"`
    Queue string `json:"Queue"`
    Args map[string]interface{} `json:"Args"`
    Enqueue_timestamp int `json:"Enqueue_timestamp"`
}

func (t *TestItem) string() string{
    return t.Name
}
*/

func job_main(){
    resq := gores.NewResQ()

    args1 := make(map[string]interface{})
    args1["id"] = 1
    timestamp1 := time.Now().Unix()
    item1 := gores.TestItem{
            Name: "TestItem",
            Queue: "TestItem",
            Args: args1,
            Enqueue_timestamp: timestamp1,
          }
    err := resq.Push("TestItem", item1)
    if err != nil{
        fmt.Printf("ResQ Push returned ERROR\n")
    }

    ret := resq.Pop("TestItem")
    fmt.Println(ret)
    if val, _ := ret["Name"]; val != "ResQ" {
        fmt.Printf("ResQ Pop Value ERROR\n")
    }

    ret2 := resq.Pop("TestItem")
    fmt.Println(ret2)
    fmt.Println(ret2 == nil)

    /* test enqueue*/
    args2 := make(map[string]interface{})
    args2["id"] = 2
    timestamp2 := time.Now().Unix()
    item2 := gores.TestItem{
            Name: "TestItem",
            Queue: "TestItem",
            Args: args2,
            Enqueue_timestamp: timestamp2,
          }

    err = resq.Enqueue(item2)
    if err != nil{
        fmt.Printf("ResQ Enqueue returned ERROR\n")
    }

    queues := resq.Queues()
    for _, q := range queues{
        fmt.Println(q)
    }

    /* test Info() */
    info := resq.Info()
    fmt.Println("ResQ Info: ")
    for k, v := range info {
        fmt.Printf("key: %s, value: %s\n", k, v)
    }

    /* test Enqueue_at()*/
    now := resq.CurrentTime()
    fmt.Println(now)
    err = resq.Enqueue_at(now, gores.TestItem{
                                  Name: "TestItem",
                                  Queue: "TestItem",
                                  Args: args2,
                                  Enqueue_timestamp: now,
                          })
    if err != nil{
        fmt.Println("Enqueue_at() Error")
    }

    next_timestamp := resq.NextDelayedTimestamp()
    fmt.Println(next_timestamp)


    next_item := resq.NextItemForTimestamp(now)
    fmt.Println(next_item)

    /* test Stat*/
    /*
    stat := gores.NewStat("TestItem", resq)
    i := stat.Get()
    fmt.Printf("stat's value: %d\n", i)

    ok := stat.Incr()
    fmt.Println(ok)
    fmt.Printf("stat's value: %d\n", stat.Get())

    ok = stat.Decr()
    fmt.Println(ok)
    fmt.Printf("stat's value %d\n", stat.Get())

    ok = stat.Clear()
    fmt.Println(ok)
    fmt.Printf("stat's value %d\n", stat.Get())
    */

    /* Test job & registry */
    gores.InitRegistry()
    fmt.Println("Perform Job: ")
    job := gores.NewJob("TestItem", ret, resq, "TestWorker")
    err = job.Perform()
    if err != nil {
        fmt.Println("Error Performing Job")
    }
}

func worker_main(){
    queues := []interface{}{"TestItem", "Comment"}
    q_set := mapset.NewSetFromSlice(queues)
    worker := gores.NewWorker(q_set)

    // test worker id
    worker_id := worker.String()
    fmt.Printf("worker id: \n%s\n", worker_id)

    // test register worker
    err := worker.RegisterWorker()
    if err != nil {
        fmt.Println("Error Register worker")
    }

    // test Exists()
    fmt.Printf("Worker: %s exists? %d\n", worker_id, worker.Exists(worker_id))

    all_workers := worker.All(worker.ResQ())
    fmt.Printf("number of workers: %d\n", len(all_workers))

    // test PruneDeadWorkers()
    worker.PruneDeadWorkers()

    // test unregiter worker
    /*
    err = worker.UnregisterWorker()
    if err != nil {
        fmt.Println("Error Unregister Worker")
    }
    */

    // test WorkerPids()
    pid_set := worker.WorkerPids()
    fmt.Printf("pid_set contains 335? %t\n", pid_set.Contains("335"))
}

func resq_main() {
    resq := gores.NewResQ()

    args1 := make(map[string]interface{})
    args1["id"] = 1
    timestamp1 := time.Now().Unix()
    item1 := gores.TestItem{
          Name: "TestItem1",
          Queue: "TestItem1",
          Args: args1,
          Enqueue_timestamp: timestamp1,
        }
    err := resq.Push("TestItem1", item1)
    if err != nil{
        fmt.Printf("ResQ Push returned ERROR\n")
    }

    queues := []interface{}{"TestItem", "TestItem1"}
    queues_set := mapset.NewSetFromSlice(queues)
    queue, ret := resq.BlockPop(queues_set)
    fmt.Println(queue)
    fmt.Println(ret)

}

func main() {
    worker_main()
}
