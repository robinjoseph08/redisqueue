package redisqueue

import "time"

type StreamItem interface {
	GetQueue() string
	GetConcurrency() int
	GetVisibilityTimeout() time.Duration
	GetBufferSize() int
}
