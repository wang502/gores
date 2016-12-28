package tests

import (
    "testing"
    "github.com/wang502/gores/gores"
)

type TestItem struct{
    Name string `json:"Name"`
}

func (t *TestItem) string() string{
    return t.Name
}

func TestResQPushPop(t *testing.T){
    resq := gores.NewResQ()
    item := TestItem{
              Name: "TestItem",
            }
    err := resq.Push("TestItem", item)
    if err != nil{
        t.Errorf("ResQ Push returned ERROR")
    }

    ret1 := resq.Pop("TestItem")
    if val, _ := ret1["Name"]; val != "TestItem"{
        t.Errorf("ResQ Pop Value ERROR")
    }

    ret2 := resq.Pop("TestItem")
    if ret2 != nil{
        t.Errorf("ResQ Pop expected to return nil, but did not")
    }
}

func TestResQSize(t *testing.T){
    resq := gores.NewResQ()
    item := TestItem{
              Name: "TestItem",
            }
    err := resq.Push("TestItem", item)
    if err != nil{
        t.Errorf("ResQ Push returned ERROR")
    }

    size := resq.Size("TestItem")
    if size != 1{
        t.Errorf("ResQ Size() expected to return 1, but returned %d", size)
    }

    resq.Pop("TestItem")
}

func TestStat(t *testing.T) {
  resq := gores.NewResQ()
  stat := gores.NewStat("TestItem", resq)
  v := stat.Get()
  if v != 0 {
      t.Errorf("Stat Get() expected to return 0 but returned %d", v)
  }

  ok := stat.Incr()
  if ok == 0 {
      t.Errorf("Stat Incr() Error")
  }

  if stat.Get() != v+1{
      t.Errorf("Stat Incr() did not increment")
  }

  ok = stat.Decr()
  if ok == 0 {
      t.Errorf("Stat Decr() Error")
  }
  if stat.Get() != v {
      t.Errorf("Stat Decr() did not decrement")
  }

  ok = stat.Clear()
}
