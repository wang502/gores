package tests

import (
    "testing"
    "github.com/wang502/gores/gores"
)

var (
    //resq declared in job_test.go
    //item declared in job_test.go
    stat = gores.NewStat("TestItem", resq)
)

func TestResQPushPop(t *testing.T){
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
