package main

import (
    "fmt"
    "github.com/wang502/gores/gores"
)

type TestItem struct{
    Name string `json:"Name"`
    Queue string `json:"Queue"`
    Args map[string]interface{} `json:"Args"`
}

func (t *TestItem) string() string{
    return t.Name
}

func main(){
    resq := gores.NewResQ()

    args1 := make(map[string]interface{})
    args1["id"] = 1
    item1 := TestItem{
            Name: "ResQ",
            Queue: "TestItem",
            Args: args1,
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
    if val, _ := ret["Struct"]; val != "TestItem" {
        fmt.Printf("ResQ Pop Value ERROR\n")
    }

    ret2 := resq.Pop("TestItem")
    fmt.Println(ret2)
    fmt.Println(ret2 == nil)

    /* test enqueue*/
    args2 := make(map[string]interface{})
    args2["id"] = 2
    item2 := TestItem{
            Name: "ResQ",
            Queue: "TestItem",
            Args: args2,
          }

    err = resq.Enqueue(item2)
    if err != nil{
        fmt.Printf("ResQ Enqueue returned ERROR\n")
    }

    queues := resq.Queues()
    for _, q := range queues{
        fmt.Println(q)
    }

    info := resq.Info()
    fmt.Println("ResQ Info: ")
    for k, v := range info {
        fmt.Printf("key: %s, value: %s\n", k, v)
    }

    /* test Stat*/
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
}
