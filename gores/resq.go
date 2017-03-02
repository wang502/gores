package gores

import (
  "encoding/json"
  "fmt"
  "os"
  "errors"
  "log"
  "strconv"
  _ "strings"
  "time"
  "github.com/garyburd/redigo/redis"
  "github.com/deckarep/golang-set"
  _ "gopkg.in/oleiade/reflections.v1"
)
// redis-cli -h host -p port -a password

// ResQ represents the main Gores object that stores all configurations and connection with Redis
type ResQ struct {
    pool *redis.Pool
    _watched_queues mapset.Set
    Host string
    config *Config
}

// NewResQ creates a new ResQ instance given the pointer to config object
func NewResQ(config *Config) *ResQ {
    var pool *redis.Pool
    var host string

    if len(config.REDISURL) != 0 && len(config.REDIS_PW) != 0 {
        pool = initPoolFromString(config.REDISURL, config.REDIS_PW)
        host = config.REDISURL
    } else {
        pool = initPool()
        host = os.Getenv("REDISURL")
    }
    if pool == nil {
        log.Printf("ERROR Initializing Redis Pool\n")
        return nil
    }
    return &ResQ{
              pool: pool,
              _watched_queues: mapset.NewSet(),
              Host: host,
              config: config,
            }
}

// NewResQ creates a new ResQ instance
// given the pointer to config object, Redis server address and password
func NewResQFromString(config *Config, server string, password string) *ResQ {
  pool := initPoolFromString(server, password)
  if pool == nil {
      log.Printf("initPool() Error\n")
      return nil
  }
  return &ResQ{
            pool: pool,
            _watched_queues: mapset.NewSet(),
            Host: os.Getenv("REDISURL"),
            config: config,
          }
}

// makeRedisPool creates new redis.Pool instance
// given Redis server address and password
func makeRedisPool(server string, password string) *redis.Pool {
    pool := &redis.Pool{
        MaxIdle: 5,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
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

// helper function to create new redis.Pool instance
func initPool() *redis.Pool{
    return makeRedisPool(os.Getenv("REDISURL"), os.Getenv("REDIS_PW"))
}

// helper function to create new redis.Pool instance
// given Redis server address and password
func initPoolFromString(server string, password string) *redis.Pool {
    return makeRedisPool(server, password)
}

// Enqueue put new job item to Redis message queue
func (resq *ResQ) Enqueue(item map[string]interface{}) error {
    /*
      Enqueue a job into a specific queue. Make sure the map you are
      passing has keys
      **Name**, **Queue**, **Enqueue_timestamp**, **Args**
    */
    queue, ok1 := item["Queue"]
    _, ok2 := item["Args"]
    var err error
    if !ok1 || !ok2 {
        err = errors.New("Unable to enqueue Job map without keys: 'Queue' and 'Args'")
    } else  {
        err = resq.push(queue.(string), item)
    }
    return err
}

// Helper function to put job item to Redis message queue
func (resq *ResQ) push(queue string, item interface{}) error{
    conn := resq.pool.Get()

    if conn == nil {
        return errors.New("Redis pool's connection is nil")
    }

    itemString, err := resq.Encode(item)
    if err != nil {
        return err
    }

    _, err = conn.Do("RPUSH", fmt.Sprintf(QUEUE_PREFIX, queue), itemString)
    if err != nil{
        err = errors.New("Invalid Redis RPUSH Response")
        return err
    }

    return resq.watch_queue(queue)
}

// Pop calls "LPOP" command on Redis message queue
// "LPOP" does not block even there is no item found
func (resq *ResQ) Pop(queue string) map[string]interface{}{
    var decoded map[string]interface{}

    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return decoded
    }

    reply, err := conn.Do("LPOP", fmt.Sprintf(QUEUE_PREFIX, queue))
    if err != nil || reply == nil {
        return decoded
    }

    data, err := redis.Bytes(reply, err)
    if err != nil{
        return decoded
    }
    item, _ := resq.Decode(data)
    return item
}

// BlockPop calls "BLPOP" command on Redis message queue
// "BLPOP" blocks for a configured time until a new job item is found and popped
func (resq *ResQ) BlockPop(queues mapset.Set) (string, map[string]interface{}) {
    var decoded map[string]interface{}

    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return "", decoded
    }

    queues_slice := make([]interface{}, queues.Cardinality())
    it := queues.Iterator()
    i := 0
    for elem := range it.C {
        queues_slice[i] = fmt.Sprintf(QUEUE_PREFIX, elem)
        i += 1
    }
    r_args := append(queues_slice, BLPOP_MAX_BLOCK_TIME)
    data, err := conn.Do("BLPOP", r_args...)

    if data == nil || err != nil {
        return "", decoded
    }

    // returned data contains [key, value], extract key at index 0, value at index 1
    queue_key := string(data.([]interface{})[0].([]byte))
    decoded, _ = resq.Decode(data.([]interface{})[1].([]byte))
    return queue_key, decoded
}

// Decode unmarshals byte array returned from Redis to a map instance
func (resq *ResQ) Decode(data []byte) (map[string]interface{}, error) {
    var decoded map[string]interface{}
    if err := json.Unmarshal(data, &decoded); err != nil{
        return decoded, err
    }
    return decoded, nil
}

// Encode marshalls map instance to its string representation
func (resq *ResQ) Encode(item interface{}) (string, error) {
    b, err := json.Marshal(item)
    if err != nil{
        return "", err
    }
    return string(b), nil
}

// Size returns the size of the given message queue "resq:queue:%s" on Redis
func (resq *ResQ) Size(queue string) int64 {
    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return 0
    }

    size, err:= conn.Do("LLEN", fmt.Sprintf(QUEUE_PREFIX, queue))
    if size == nil || err != nil {
        return 0
    }
    return size.(int64)
}

// SizeOfQueue return the size of any given queue on Redis
func (resq *ResQ) SizeOfQueue(key string) int64{
    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return 0
    }

    size, err := conn.Do("LLEN", key)
    if size == nil || err != nil {
        return 0
    }
    return size.(int64)
}

func (resq *ResQ) watch_queue(queue string) error{
    if resq._watched_queues.Contains(queue){
        return nil
    } else {
        conn := resq.pool.Get()
        if conn == nil {
            return errors.New("Redis pool's connection is nil")
        }

        _, err := conn.Do("SADD", WATCHED_QUEUES, queue)
        if err != nil{
            err = errors.New("watch_queue() SADD Error")
        }
        return err
    }
}

func (resq *ResQ) Enqueue_at(datetime int64, item interface{}) error {
    err := resq.delayedPush(datetime, item)
    if err != nil {
        return err
    }
    return nil
}

func (resq *ResQ) delayedPush(datetime int64, item interface{}) error {
    conn := resq.pool.Get()
    if conn == nil {
        return errors.New("Redis pool's connection is nil")
    }

    key := strconv.FormatInt(datetime, 10)
    itemString, err := resq.Encode(item)
    if err != nil {
        return err
    }

    _, err = conn.Do("RPUSH", fmt.Sprintf(DEPLAYED_QUEUE_PREFIX, key), itemString)
    if err != nil {
        return errors.New("Invalid RPUSH response")
    }

    _, err = conn.Do("ZADD", WATCHED_DELAYED_QUEUE_SCHEDULE, datetime, datetime)
    if err != nil {
        err = errors.New("Invalid ZADD response")
    }
    return err
}

func (resq *ResQ) Queues() []string{
    queues := make([]string, 0)

    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return queues
    }

    data, _ := conn.Do("SMEMBERS", WATCHED_QUEUES)
    for _, q := range data.([]interface{}){
        queues = append(queues, string(q.([]byte)))
    }
    return queues
}

func (resq *ResQ) Workers() []string {
    conn := resq.pool.Get()
    data, err := conn.Do("SMEMBERS", WATCHED_WORKERS)
    if data == nil || err != nil {
        return nil
    }

    workers := make([]string, len(data.([]interface{})))
    for i, w := range data.([]interface{}) {
        workers[i] = string(w.([]byte))
    }
    return workers
}

func (resq *ResQ) Info() map[string]interface{} {
    var pending int64 = 0
    for _, q := range resq.Queues() {
        pending += resq.Size(q)
    }

    info := make(map[string]interface{})
    info["pending"] = pending
    info["processed"] = NewStat("processed", resq).Get()
    info["queues"] = len(resq.Queues())
    info["workers"] = len(resq.Workers())
    info["failed"] = NewStat("falied", resq).Get()
    info["host"] = resq.Host
    return info
}

func (resq *ResQ) NextDelayedTimestamp() int64 {
    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return 0
    }

    key := resq.CurrentTime()
    data, err := conn.Do("ZRANGEBYSCORE", WATCHED_DELAYED_QUEUE_SCHEDULE, "-inf", key)
    if err != nil || data == nil {
        return 0
    }
    if len(data.([]interface{})) > 0 {
        bytes := make([]byte, len(data.([]interface{})[0].([]uint8)))
        for i, v := range data.([]interface{})[0].([]uint8) {
            bytes[i] = byte(v)
        }
        res, _ :=  strconv.Atoi(string(bytes))
        return int64(res)
    }
    return 0
}

func (resq *ResQ) NextItemForTimestamp(timestamp int64) map[string]interface{} {
    var res map[string]interface{}

    s_time := strconv.FormatInt(timestamp, 10)
    key := fmt.Sprintf(DEPLAYED_QUEUE_PREFIX, s_time)

    conn := resq.pool.Get()
    if conn == nil {
        log.Printf("Redis pool's connection is nil")
        return res
    }

    reply, err := conn.Do("LPOP", key)
    if reply == nil || err != nil {
        return res
    }
    data, err := redis.Bytes(reply, err)
    if err != nil {
        return res
    }
    res, _ = resq.Decode(data)
    llen, err := conn.Do("LLEN", key)
    if llen == nil || err != nil {
        return res
    }
    if llen.(int64) == 0 {
        conn.Do("DEL", key)
        conn.Do("ZREM", WATCHED_DELAYED_QUEUE_SCHEDULE, timestamp)
    }
    return res
}

func (resq *ResQ) CurrentTime() int64 {
    timestamp := time.Now().Unix()
    return timestamp
}

/* -------------------------------------------------------------------------- */
// Launch startups the gores Dispatcher and Worker to do background works
func Launch(config *Config, tasks *map[string]interface{}) error {
    resq := NewResQ(config)
    if resq == nil {
        return errors.New("ResQ is nil")
    }

    in_slice := make([]interface{}, len(config.Queues))
    for i, q := range config.Queues {
        in_slice[i] = q
    }
    queues_set := mapset.NewSetFromSlice(in_slice)

    dispatcher := NewDispatcher(resq, config, queues_set)
    if dispatcher == nil {
        return errors.New("Dispatcher is nil")
    }
    err := dispatcher.Run(tasks)
    return err
}
