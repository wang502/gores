package gores

import (
	"fmt"
	"log"
	"strconv"
)

// Stat represents the statistic of a specific job queue in Redis
type Stat struct {
	name  string
	key   string
	gores *Gores
}

// NewStat initializes a new stat struct
func NewStat(name string, gores *Gores) *Stat {
	return &Stat{
		name:  name,
		key:   fmt.Sprintf(statPrefix, name),
		gores: gores,
	}
}

// Get retrieves the statistic of the given queue
func (stat *Stat) Get() int64 {
	conn := stat.gores.pool.Get()
	data, err := conn.Do("GET", stat.key)
	if err != nil || data == nil {
		return 0
	}
	res, _ := strconv.Atoi(string(data.([]byte)))
	return int64(res)
}

// Incr increments the count of the given queue key
func (stat *Stat) Incr() int {
	_, err := stat.gores.pool.Get().Do("INCR", stat.key)
	if err != nil {
		log.Println(err)
		return 0
	}
	return 1
}

// Decr decrements the count of the given queue key
func (stat *Stat) Decr() int {
	_, err := stat.gores.pool.Get().Do("DECR", stat.key)
	if err != nil {
		log.Println(err)
		return 0
	}
	return 1
}

// Clear deletes the statistic about the queue
func (stat *Stat) Clear() int {
	_, err := stat.gores.pool.Get().Do("DEL", stat.key)
	if err != nil {
		log.Println(err)
		return 0
	}
	return 1
}
