package gores

import (
	"fmt"
	"log"
	"strconv"
)

// Stat represents the statistic of a specific job queue in Redis
type Stat struct {
	Name string
	Key  string
	Resq *ResQ
}

// NewStat initializes a new stat struct
func NewStat(name string, resq *ResQ) *Stat {
	return &Stat{
		Name: name,
		Key:  fmt.Sprintf(statPrefix, name),
		Resq: resq,
	}
}

// Get retrieves the statistic of the given queue
func (stat *Stat) Get() int64 {
	conn := stat.Resq.pool.Get()
	data, err := conn.Do("GET", stat.Key)
	if err != nil || data == nil {
		return 0
	}
	res, _ := strconv.Atoi(string(data.([]byte)))
	return int64(res)
}

// Incr increments the count of the given queue key
func (stat *Stat) Incr() int {
	_, err := stat.Resq.pool.Get().Do("INCR", stat.Key)
	if err != nil {
		log.Println(err)
		return 0
	}
	return 1
}

// Decr decrements the count of the given queue key
func (stat *Stat) Decr() int {
	_, err := stat.Resq.pool.Get().Do("DECR", stat.Key)
	if err != nil {
		log.Println(err)
		return 0
	}
	return 1
}

// Clear deletes the statistic about the queue
func (stat *Stat) Clear() int {
	_, err := stat.Resq.pool.Get().Do("DEL", stat.Key)
	if err != nil {
		log.Println(err)
		return 0
	}
	return 1
}
