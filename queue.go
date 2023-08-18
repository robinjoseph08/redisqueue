package redisqueue

type StreamItem interface {
	GetQueue() string
	GetConcurrency() int
}
