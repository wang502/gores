package gores

import (
    "fmt"
    "strconv"
    "log"
)

type Stat struct{
    Name string
    Key string
    Resq *ResQ
}

func NewStat(name string, resq *ResQ) *Stat {
    return &Stat{
              Name: name,
              Key: fmt.Sprintf(STAT_PREFIX, name),
              Resq: resq,
          }
}

func (stat *Stat) Get() int64 {
    conn := stat.Resq.pool.Get()
    data, err := conn.Do("GET", stat.Key)
    if err != nil || data == nil{
      return 0
    }
    res, _ := strconv.Atoi(string(data.([]byte)))
    return int64(res)
}

func (stat *Stat) Incr() int{
    _, err:= stat.Resq.pool.Get().Do("INCR", stat.Key)
    if err != nil{
        log.Println(err)
        return 0
    }
    return 1
}

func (stat *Stat) Decr() int {
    _, err:= stat.Resq.pool.Get().Do("DECR", stat.Key)
    if err != nil{
        log.Println(err)
        return 0
    }
    return 1
}

func (stat *Stat) Clear() int{
    _, err:= stat.Resq.pool.Get().Do("DEL", stat.Key)
    if err != nil{
      log.Println(err)
      return 0
    }
    return 1
}
