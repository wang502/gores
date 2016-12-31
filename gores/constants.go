package gores

const (
   // QUEUE
   QUEUE_PREFIX = "resq:queue:%s"
   WORKER_PREFIX = "resq:worker:%s"
   DEPLAYED_QUEUE_PREFIX = "resq:delayed:%s"
   STAT_PREFIX = "resq:stat:%s"

   // SET
   WATCHED_QUEUES = "resq:queues"
   WATCHED_DELAYED_QUEUE_SCHEDULE = "resq:delayed_queue_schedule"
   WATCHED_WORKERS = "resq:workers"

   BLPOP_MAX_BLOCK_TIME = 1 /* Redis BLPOP maximum block time */

   MAX_WORKERS = 10 /* Maximum workers */

)
