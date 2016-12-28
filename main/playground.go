package main

import (
    "fmt"
    "strconv"
    "time"
    "github.com/wang502/gores/gores"
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

func main(){
    resq := gores.NewResQ()

    args1 := make(map[string]interface{})
    args1["id"] = 1
    timestamp1, _ := strconv.Atoi(time.Now().Format("20060102150405"))
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
    timestamp2, err := strconv.Atoi(time.Now().Format("20060102150405"))
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
