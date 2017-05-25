package gores

const (
	// QUEUE
	queuePrefix        = "gores:queue:%s"
	workerPrefix       = "gores:worker:%s"
	delayedQueuePrefix = "gores:delayed:%s"
	statPrefix         = "gores:stat:%s"

	// SET
	watchedQueues    = "gores:queues"
	watchedSchedules = "gores:delayed_queue_schedule"
	watchedWorkers   = "gores:workers"

	blpopMaxBlockTime = 1 /* Redis BLPOP maximum block time */

	maxWorkers = 10 /* Maximum workers */

)
