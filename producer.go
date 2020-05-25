package redisqueue

import (
	"github.com/go-redis/redis"
)

// ProducerOptions provide options to configure the Producer.
type ProducerOptions struct {
	// StreamMaxLength sets the MAXLEN option when calling XADD. This creates a
	// capped stream to prevent the stream from taking up memory indefinitely.
	// It's important to note though that this isn't the maximum number of
	// _completed_ messages, but the maximum number of _total_ messages. This
	// means that if all consumers are down, but producers are still enqueuing,
	// and the maximum is reached, unprocessed message will start to be dropped.
	// So ideally, you'll set this number to be as high as you can makee it.
	// More info here: https://redis.io/commands/xadd#capped-streams.
	StreamMaxLength int64
	// ApproximateMaxLength determines whether to use the ~ with the MAXLEN
	// option. This allows the stream trimming to done in a more efficient
	// manner. More info here: https://redis.io/commands/xadd#capped-streams.
	ApproximateMaxLength bool
	// RedisClient supersedes the RedisOptions field, and allows you to inject
	// an already-made *redis.Client for use in the consumer.
	RedisClient *redis.Client
	// RedisOptions allows you to configure the underlying Redis connection.
	// More info here: https://godoc.org/github.com/go-redis/redis#Options.
	//
	// This field is used if RedisClient field is nil.
	RedisOptions *RedisOptions
}

// Producer adds a convenient wrapper around enqueuing messages that will be
// processed later by a Consumer.
type Producer struct {
	options *ProducerOptions
	redis   *redis.Client
}

var defaultProducerOptions = &ProducerOptions{
	StreamMaxLength:      1000,
	ApproximateMaxLength: true,
}

// NewProducer uses a default set of options to create a Producer. It sets
// StreamMaxLength to 1000 and ApproximateMaxLength to true. In most production
// environments, you'll want to use NewProducerWithOptions.
func NewProducer() (*Producer, error) {
	return NewProducerWithOptions(defaultProducerOptions)
}

// NewProducerWithOptions creates a Producer using custom ProducerOptions.
func NewProducerWithOptions(options *ProducerOptions) (*Producer, error) {
	var r *redis.Client

	if options.RedisClient != nil {
		r = options.RedisClient
	} else {
		r = newRedisClient(options.RedisOptions)
	}

	if err := redisPreflightChecks(r); err != nil {
		return nil, err
	}

	return &Producer{
		options: options,
		redis:   r,
	}, nil
}

// Enqueue takes in a pointer to Message and enqueues it into the stream set at
// msg.Stream. While you can set msg.ID, unless you know what you're doing, you
// should let Redis auto-generate the ID. If an ID is auto-generated, it will be
// set on msg.ID for your reference. msg.Values is also required.
func (p *Producer) Enqueue(msg *Message) error {
	args := &redis.XAddArgs{
		ID:     msg.ID,
		Stream: msg.Stream,
		Values: msg.Values,
	}
	if p.options.ApproximateMaxLength {
		args.MaxLenApprox = p.options.StreamMaxLength
	} else {
		args.MaxLen = p.options.StreamMaxLength
	}
	id, err := p.redis.XAdd(args).Result()
	if err != nil {
		return err
	}
	msg.ID = id
	return nil
}
