package gores

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/deckarep/golang-set"
	"github.com/garyburd/redigo/redis"
)

// redis-cli -h host -p port -a password

// Gores represents the main Gores object that stores all configurations and connection with Redis
type Gores struct {
	pool          *redis.Pool
	watchedQueues mapset.Set
	host          string
	config        *Config
}

// NewGores creates a new Gores instance given the pointer to config object
func NewGores(config *Config) *Gores {
	var pool *redis.Pool
	var host string

	pool = initPool(config)
	host = config.RedisURL

	if pool == nil {
		log.Printf("ERROR Initializing Redis Pool\n")
		return nil
	}

	return &Gores{
		pool:          pool,
		watchedQueues: mapset.NewSet(),
		host:          host,
		config:        config,
	}
}

// helper function to create new redis.Pool instance
func initPool(config *Config) *redis.Pool {
	return newRedisPoool(config)
}

// newRedisPoool creates new redis.Pool instance
// given Redis server address and password
func newRedisPoool(config *Config) *redis.Pool {
	server := config.RedisURL
	password := config.RedisPassword
	maxIdle := config.RedisMaxIdle
	idleTimeout := config.RedisIdleTimeout

	pool := &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: time.Duration(idleTimeout) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return c, nil
			}
			c.Do("AUTH", password)

			/* the is needed only if "gores" is configured in Redis's configuration file redis.conf */
			//c.Do("SELECT", "gores")
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return pool
}

// Enqueue puts new job item to Redis message queue
func (gores *Gores) Enqueue(item map[string]interface{}) error {
	/*
	   Enqueue a job into a specific queue. Make sure the map you are
	   passing has keys
	   **Name**, **Queue**, **Enqueue_timestamp**, **Args**
	*/
	queue, ok1 := item["Queue"]
	_, ok2 := item["Args"]
	if !ok1 || !ok2 {
		return errors.New("enqueue item failed: job item has no key 'Queue' or 'Args'")
	}

	err := gores.push(queue.(string), item)
	if err != nil {
		return fmt.Errorf("enqueue item failed: %s", err)
	}

	return nil
}

// Helper function to put job item to Redis message queue
func (gores *Gores) push(queue string, item interface{}) error {
	conn := gores.pool.Get()
	defer conn.Close()

	itemString, err := gores.Encode(item)
	if err != nil {
		return fmt.Errorf("push item failed: %s", err)
	}

	_, err = conn.Do("RPUSH", fmt.Sprintf(queuePrefix, queue), itemString)
	if err != nil {
		return fmt.Errorf("push item failed: %s", err)
	}

	err = gores.watchQueue(queue)
	if err != nil {
		return fmt.Errorf("push item failed: %s", err)
	}

	return nil
}

// Pop calls "LPOP" command on Redis message queue
// "LPOP" does not block even there is no item found
func (gores *Gores) Pop(queue string) (map[string]interface{}, error) {
	conn := gores.pool.Get()
	defer conn.Close()

	reply, err := conn.Do("LPOP", fmt.Sprintf(queuePrefix, queue))
	if err != nil {
		return nil, fmt.Errorf("pop failed: %s", err)
	}
	if reply == nil {
		return nil, nil
	}

	data, err := redis.Bytes(reply, err)
	if err != nil {
		return nil, fmt.Errorf("pop failed: %s", err)
	}

	return gores.Decode(data)
}

// BlockPop calls "BLPOP" command on Redis message queue
// "BLPOP" blocks for a configured time until a new job item is found and popped
func (gores *Gores) BlockPop(queues mapset.Set) (string, map[string]interface{}, error) {
	conn := gores.pool.Get()
	defer conn.Close()

	queuesSlice := make([]interface{}, queues.Cardinality())
	it := queues.Iterator()
	i := 0
	for elem := range it.C {
		queuesSlice[i] = fmt.Sprintf(queuePrefix, elem)
		i++
	}
	redisArgs := append(queuesSlice, blpopMaxBlockTime)
	data, err := conn.Do("BLPOP", redisArgs...)
	if err != nil {
		return "", nil, fmt.Errorf("blpop failed: %s", err)
	}
	if data == nil {
		return "", nil, nil
	}

	// returned data contains [key, value], extract key at index 0, value at index 1
	queueKey := string(data.([]interface{})[0].([]byte))
	decoded, _ := gores.Decode(data.([]interface{})[1].([]byte))
	return queueKey, decoded, nil
}

// Decode unmarshals byte array returned from Redis to a map instance
func (gores *Gores) Decode(data []byte) (map[string]interface{}, error) {
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return decoded, fmt.Errorf("decode data failed: %s", err)
	}

	return decoded, nil
}

// Encode marshalls map instance to its string representation
func (gores *Gores) Encode(item interface{}) (string, error) {
	b, err := json.Marshal(item)
	if err != nil {
		return "", fmt.Errorf("encode data failed: %s", err)
	}

	return string(b), nil
}

// Size returns the size of the given message queue "gores:queue:%s" on Redis
func (gores *Gores) Size(queue string) (int64, error) {
	conn := gores.pool.Get()
	defer conn.Close()

	size, err := conn.Do("LLEN", fmt.Sprintf(queuePrefix, queue))
	if size == nil || err != nil {
		return 0, fmt.Errorf("Gores find queue size failed: %s", err)
	}

	return size.(int64), nil
}

// SizeOfQueue return the size of any given queue on Redis
func (gores *Gores) SizeOfQueue(key string) int64 {
	conn := gores.pool.Get()
	defer conn.Close()

	size, err := conn.Do("LLEN", key)
	if size == nil || err != nil {
		return 0
	}

	return size.(int64)
}

func (gores *Gores) watchQueue(queue string) error {
	if gores.watchedQueues.Contains(queue) {
		return nil
	}
	conn := gores.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SADD", watchedQueues, queue)
	if err != nil {
		return fmt.Errorf("watch queue failed: %s", err)
	}

	return nil
}

// EnqueueAt puts the job to Redis delayed queue for the given timestamp
func (gores *Gores) EnqueueAt(datetime int64, item interface{}) error {
	err := gores.delayedPush(datetime, item)
	if err != nil {
		return fmt.Errorf("enqueue at timestamp failed: %s", err)
	}

	return nil
}

func (gores *Gores) delayedPush(datetime int64, item interface{}) error {
	conn := gores.pool.Get()
	defer conn.Close()

	key := strconv.FormatInt(datetime, 10)
	itemString, err := gores.Encode(item)
	if err != nil {
		return fmt.Errorf("delayed push failed: %s", err)
	}

	_, err = conn.Do("RPUSH", fmt.Sprintf(delayedQueuePrefix, key), itemString)
	if err != nil {
		return fmt.Errorf("delayed push failed: %s", err)
	}

	_, err = conn.Do("ZADD", watchedSchedules, datetime, datetime)
	if err != nil {
		return fmt.Errorf("delayed push failed: %s", err)
	}

	return nil
}

// Queues returns a slice of existing queues' names
func (gores *Gores) Queues() []string {
	queues := make([]string, 0)

	conn := gores.pool.Get()
	defer conn.Close()

	data, _ := conn.Do("SMEMBERS", watchedQueues)
	for _, q := range data.([]interface{}) {
		queues = append(queues, string(q.([]byte)))
	}

	return queues
}

// Workers retruns a slice of existing worker names
func (gores *Gores) Workers() []string {
	conn := gores.pool.Get()
	defer conn.Close()

	data, err := conn.Do("SMEMBERS", watchedWorkers)
	if data == nil || err != nil {
		return nil
	}

	workers := make([]string, len(data.([]interface{})))
	for i, w := range data.([]interface{}) {
		workers[i] = string(w.([]byte))
	}

	return workers
}

// Info returns the information of the Redis queue
func (gores *Gores) Info() (map[string]interface{}, error) {
	var pending int64
	for _, q := range gores.Queues() {
		num, err := gores.Size(q)
		if err != nil {
			return nil, fmt.Errorf("Gores info failed: %s", err)
		}

		pending += num
	}

	info := make(map[string]interface{})
	info["pending"] = pending
	info["processed"] = NewStat("processed", gores).Get()
	info["queues"] = len(gores.Queues())
	info["workers"] = len(gores.Workers())
	info["failed"] = NewStat("falied", gores).Get()
	info["host"] = gores.host

	return info, nil
}

// NextDelayedTimestamp returns the next delayed timestamps
func (gores *Gores) NextDelayedTimestamp() int64 {
	conn := gores.pool.Get()
	defer conn.Close()

	key := gores.CurrentTime()
	data, err := conn.Do("ZRANGEBYSCORE", watchedSchedules, "-inf", key)
	if err != nil || data == nil {
		return 0
	}
	if len(data.([]interface{})) > 0 {
		bytes := make([]byte, len(data.([]interface{})[0].([]uint8)))
		for i, v := range data.([]interface{})[0].([]uint8) {
			bytes[i] = byte(v)
		}
		res, _ := strconv.Atoi(string(bytes))
		return int64(res)
	}
	return 0
}

// NextItemForTimestamp fetches item from delayed queue in Redis that has the given timestamp
func (gores *Gores) NextItemForTimestamp(timestamp int64) map[string]interface{} {
	var res map[string]interface{}

	timeStr := strconv.FormatInt(timestamp, 10)
	key := fmt.Sprintf(delayedQueuePrefix, timeStr)

	conn := gores.pool.Get()
	defer conn.Close()

	reply, err := conn.Do("LPOP", key)
	if reply == nil || err != nil {
		return res
	}
	data, err := redis.Bytes(reply, err)
	if err != nil {
		return res
	}
	res, _ = gores.Decode(data)
	llen, err := conn.Do("LLEN", key)
	if llen == nil || err != nil {
		return res
	}
	if llen.(int64) == 0 {
		conn.Do("DEL", key)
		conn.Do("ZREM", watchedSchedules, timestamp)
	}
	return res
}

// CurrentTime retruns the current unix timestamp
func (gores *Gores) CurrentTime() int64 {
	timestamp := time.Now().Unix()
	return timestamp
}

/* -------------------------------------------------------------------------- */

// Launch startups the gores Dispatcher and Worker to do background works
func Launch(config *Config, tasks *map[string]interface{}) error {
	gores := NewGores(config)
	if gores == nil {
		return errors.New("Gores launch failed: Gores is nil")
	}

	inSlice := make([]interface{}, len(config.Queues))
	for i, q := range config.Queues {
		inSlice[i] = q
	}
	queuesSet := mapset.NewSetFromSlice(inSlice)

	dispatcher := NewDispatcher(gores, config, queuesSet)
	if dispatcher == nil {
		return errors.New("Gores launch failed: Dispatcher is nil")
	}

	err := dispatcher.Start(tasks)
	if err != nil {
		return fmt.Errorf("Gores launch failed: %s", err)
	}

	return nil
}
