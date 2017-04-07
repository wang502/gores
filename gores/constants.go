package gores

const (
	// QUEUE
	queuePrefix        = "resq:queue:%s"
	workerPrefix       = "resq:worker:%s"
	delayedQueuePrefix = "resq:delayed:%s"
	statPrefix         = "resq:stat:%s"

	// SET
	watchedQueues    = "resq:queues"
	watchedSchedules = "resq:delayed_queue_schedule"
	watchedWorkers   = "resq:workers"

	blpopMaxBlockTime = 1 /* Redis BLPOP maximum block time */

	maxWorkers = 10 /* Maximum workers */

)
