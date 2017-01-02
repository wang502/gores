package gores

import (
  "encoding/json"
  "fmt"
  "os"
  "errors"
  "runtime"
  "path"
  "strconv"
  _ "strings"
  "time"
  "github.com/garyburd/redigo/redis"
  "github.com/deckarep/golang-set"
  "gopkg.in/oleiade/reflections.v1"
)
// redis-cli -h host -p port -a password

type ResQ struct {
  pool *redis.Pool
  _watched_queues mapset.Set
  Host string
}

func MakeRedisPool(server string, password string) *redis.Pool {
    pool := &redis.Pool{
        MaxIdle: 5,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
              c, err := redis.Dial("tcp", os.Getenv("REDISURL"))
              if err != nil {
                  return c, nil
              }
              c.Do("AUTH", os.Getenv("REDIS_PW"))
              c.Do("SELECT", "gores")
              return c, nil
            },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
    return pool
}

func InitPool() *redis.Pool{
    return MakeRedisPool(os.Getenv("REDISURL"), os.Getenv("REDIS_PW"))
}

func InitPoolFromString(server string, password string) *redis.Pool {
    return MakeRedisPool(server, password)
}

func NewResQ() *ResQ {
    var pool *redis.Pool
    var host string

    config, err := InitConfig()
    if err != nil {
        fmt.Printf("ERROR Initializing Config && %s\n", err)
        return nil
    }
    if len(config.REDISURL) != 0 && len(config.REDIS_PW) != 0 {
        pool = InitPoolFromString(config.REDISURL, config.REDIS_PW)
        host = config.REDISURL
    } else {
        pool = InitPool()
        host = os.Getenv("REDISURL")
    }
    if pool == nil {
        fmt.Printf("ERROR Initializing Redis Pool\n")
        return nil
    }
    return &ResQ{
              pool: pool,
              _watched_queues: mapset.NewSet(),
              Host: host,
            }
}

func NewResQFromString(server string, password string) *ResQ {
  pool := InitPoolFromString(server, password)
  if pool == nil {
      fmt.Printf("InitPool() Error\n")
      return nil
  }
  return &ResQ{
            pool: pool,
            _watched_queues: mapset.NewSet(),
            Host: os.Getenv("REDISURL"),
          }
}

func (resq *ResQ) Push(queue string, item interface{}) error{
    conn := resq.pool.Get()

    _, err := conn.Do("RPUSH", fmt.Sprintf(QUEUE_PREFIX, queue), resq.Encode(item))
    if err != nil{
        err = errors.New("Invalid Redis RPUSH Response")
    }
    if err != nil{
        return err
    }
    err = resq.watch_queue(queue)
    if err != nil{
        return err
    }
    return nil
}

func (resq *ResQ) Pop(queue string) map[string]interface{}{
    var decoded map[string]interface{}

    conn := resq.pool.Get()
    reply, err := conn.Do("LPOP", fmt.Sprintf(QUEUE_PREFIX, queue))
    if err != nil || reply == nil {
        return decoded
    }

    data, err := redis.Bytes(reply, err)
    if err != nil{
        return decoded
    }
    decoded = resq.Decode(data)
    if decoded != nil{
        decoded["Struct"] = queue
    }
    return decoded
}

func (resq *ResQ) BlockPop(queues mapset.Set) (string, map[string]interface{}) {
    var decoded map[string]interface{}

    conn := resq.pool.Get()
    queues_slice := make([]interface{}, queues.Cardinality())
    it := queues.Iterator()
    i := 0
    for elem := range it.C {
        queues_slice[i] = fmt.Sprintf(QUEUE_PREFIX, elem)
        i += 1
    }
    r_args := append(queues_slice, BLPOP_MAX_BLOCK_TIME)
    data, err := conn.Do("BLPOP", r_args...) // block time: 1

    if data == nil || err != nil {
        return "", decoded
    }

    // returned data contains [key, value], extract key at index 0, value at index 1
    queue_key := string(data.([]interface{})[0].([]byte))
    decoded = resq.Decode(data.([]interface{})[1].([]byte))
    return queue_key, decoded
}

func (resq *ResQ) Decode(data []byte) map[string]interface{}{
    var decoded map[string]interface{}
    if err := json.Unmarshal(data, &decoded); err != nil{
        return decoded
    }
    return decoded
}

func (resq *ResQ) Encode(item interface{}) string{
    b, err := json.Marshal(item)
    if err != nil{
        return ""
    }
    return string(b)
}

func (resq *ResQ) Size(queue string) int64 {
    conn := resq.pool.Get()
    size, err:= conn.Do("LLEN", fmt.Sprintf(QUEUE_PREFIX, queue))
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
        _, err := conn.Do("SADD", WATCHED_QUEUES, queue)
        if err != nil{
            err = errors.New("watch_queue() SADD Error")
        }
        return err
    }
}

func (resq *ResQ) Enqueue(item interface{}) error{
    /*
    Enqueue a job into a specific queue. Make sure the struct you are
    passing has
    **Queue**, **Enqueue_timestamp**, **Args** attribute
    and a
    **Perform** method on it.
    */
    hasQueue, _ := reflections.HasField(item, "Queue")
    hasArgs, _ := reflections.HasField(item, "Args")
    if !hasQueue || !hasArgs {
        return errors.New("unable to enqueue Job without 'Queue' and 'Args' attributes")
    } else {
        queue, _ := reflections.GetField(item, "Queue")
        err := resq.Push(queue.(string), item)
        return err
    }
}

func (resq *ResQ) EnqueueDelayedItem(item map[string]interface{}) error {
    queue, ok1 := item["Queue"]
    _, ok2 := item["Args"]
    var err error
    if !ok1 || !ok2 {
        err = errors.New("unable to enqueue delayed Job without 'Queue' and 'Args' attributes")
    } else  {
        err = resq.Push(queue.(string), item)
    }
    return err
}

func (resq *ResQ) Queues() []string{
    conn := resq.pool.Get()
    data, _ := conn.Do("SMEMBERS", WATCHED_QUEUES)
    queues := make([]string, 0)
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

func (resq *ResQ) Enqueue_at(datetime int64, item interface{}) error {
    err := resq.DelayedPush(datetime, item)
    if err != nil {
        return err
    }
    return nil
}

func (resq *ResQ) DelayedPush(datetime int64, item interface{}) error {
    conn := resq.pool.Get()
    key := strconv.FormatInt(datetime, 10)
    _, err := conn.Do("RPUSH", fmt.Sprintf(DEPLAYED_QUEUE_PREFIX, key), resq.Encode(item))
    if err != nil {
        return errors.New("Invalid RPUSH response")
    }
    _, err = conn.Do("ZADD", WATCHED_DELAYED_QUEUE_SCHEDULE, datetime, datetime)
    if err != nil {
        err = errors.New("Invalid ZADD response")
    }
    return err
}

func (resq *ResQ) NextDelayedTimestamp() int64 {
    conn := resq.pool.Get()
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
    reply, err := conn.Do("LPOP", key)
    if reply == nil || err != nil {
        return res
    }
    data, err := redis.Bytes(reply, err)
    if err != nil {
        return res
    }
    res = resq.Decode(data)
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
        return 0
    }
    return 1
}

func (stat *Stat) Decr() int {
    _, err:= stat.Resq.pool.Get().Do("DECR", stat.Key)
    if err != nil{
        return 0
    }
    return 1
}

func (stat *Stat) Clear() int{
    _, err:= stat.Resq.pool.Get().Do("DEL", stat.Key)
    if err != nil{
      return 0
    }
    return 1
}

/* -------------------------------------------------------------------------- */

type Config struct {
    REDISURL string
    REDIS_PW string
    BLPOP_MAX_BLOCK_TIME int
    MAX_WORKERS int
    Queues []string
}

func InitConfig() (*Config, error) {
    config := Config{}

    _, filename, _, _ := runtime.Caller(0)
    config_file, err := os.Open(path.Dir(filename) + "/config.json")
    if err != nil {
        return &config, err
    }

    decoder := json.NewDecoder(config_file)
    err = decoder.Decode(&config)
    if err != nil {
        return &config, errors.New("ERROR decode config.json")
    }
    return &config, nil
}

/* -------------------------------------------------------------------------- */

func Launch() {
    config, err := InitConfig()
    if err != nil {
        fmt.Println(err)
        return
    }
    InitRegistry()
    resq := NewResQ()

    in_slice := make([]interface{}, len(config.Queues))
    for i, q := range config.Queues {
        in_slice[i] = q
    }
    queues_set := mapset.NewSetFromSlice(in_slice)

    dispatcher := NewDispatcher(resq, config.MAX_WORKERS, queues_set)
    dispatcher.Run()
}
