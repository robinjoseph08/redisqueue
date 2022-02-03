package redisqueue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumer(t *testing.T) {
	t.Run("creates a new consumer", func(tt *testing.T) {
		c, err := NewConsumer()
		require.NoError(tt, err)

		assert.NotNil(tt, c)
	})
}

func TestNewConsumerWithOptions(t *testing.T) {
	t.Run("creates a new consumer", func(tt *testing.T) {
		c, err := NewConsumerWithOptions(&ConsumerOptions{})
		require.NoError(tt, err)

		assert.NotNil(tt, c)
	})

	t.Run("sets defaults for Name, GroupName, BlockingTimeout, and ReclaimTimeout", func(tt *testing.T) {
		c, err := NewConsumerWithOptions(&ConsumerOptions{})
		require.NoError(tt, err)

		hostname, err := os.Hostname()
		require.NoError(tt, err)

		assert.Equal(tt, hostname, c.options.Name)
		assert.Equal(tt, "redisqueue", c.options.GroupName)
		assert.Equal(tt, 5*time.Second, c.options.BlockingTimeout)
		assert.Equal(tt, 1*time.Second, c.options.ReclaimInterval)
	})

	t.Run("allows override of Name, GroupName, BlockingTimeout, ReclaimTimeout, and RedisClient", func(tt *testing.T) {
		rc := newRedisClient(nil)

		c, err := NewConsumerWithOptions(&ConsumerOptions{
			Name:            "test_name",
			GroupName:       "test_group_name",
			BlockingTimeout: 10 * time.Second,
			ReclaimInterval: 10 * time.Second,
			RedisClient:     rc,
		})
		require.NoError(tt, err)

		assert.Equal(tt, rc, c.redis)
		assert.Equal(tt, "test_name", c.options.Name)
		assert.Equal(tt, "test_group_name", c.options.GroupName)
		assert.Equal(tt, 10*time.Second, c.options.BlockingTimeout)
		assert.Equal(tt, 10*time.Second, c.options.ReclaimInterval)
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		_, err := NewConsumerWithOptions(&ConsumerOptions{
			RedisOptions: &RedisOptions{Addr: "localhost:0"},
		})
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}

func TestRegister(t *testing.T) {
	fn := func(msg *Message) error {
		return nil
	}

	t.Run("set the function", func(tt *testing.T) {
		c, err := NewConsumer()
		require.NoError(tt, err)

		c.Register(tt.Name(), fn)

		assert.Len(tt, c.consumers, 1)
	})
}

func TestRegisterWithLastID(t *testing.T) {
	fn := func(msg *Message) error {
		return nil
	}

	tests := []struct {
		name   string
		stream string
		id     string
		want   map[string]registeredConsumer
	}{
		{
			name: "custom_id",
			id:   "42",
			want: map[string]registeredConsumer{
				"test": {id: "42", fn: fn},
			},
		},
		{
			name: "no_id",
			id:   "",
			want: map[string]registeredConsumer{
				"test": {id: "0", fn: fn},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer()
			require.NoError(t, err)

			c.RegisterWithLastID("test", tt.id, fn)

			assert.Len(t, c.consumers, 1)
			assert.Contains(t, c.consumers, "test")
			assert.Equal(t, c.consumers["test"].id, tt.want["test"].id)
			assert.NotNil(t, c.consumers["test"].fn)
		})
	}
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("sends an error if no ConsumerFuncs are registered", func(tt *testing.T) {
		c, err := NewConsumer()
		require.NoError(tt, err)

		go func() {
			err := <-c.Errors
			require.Error(tt, err)
			assert.Equal(tt, "at least one consumer function needs to be registered", err.Error())
		}()

		c.Run()
	})

	t.Run("calls the ConsumerFunc on for a message", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer(ctx)
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(ctx, tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(ctx, tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			c.Shutdown()
			return nil
		})

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// run the consumer
		c.Run()
	})

	t.Run("reclaims pending messages according to ReclaimInterval", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 5 * time.Millisecond,
			BlockingTimeout:   10 * time.Millisecond,
			ReclaimInterval:   1 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer(ctx)
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(ctx, tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(ctx, tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, msg.ID, m.ID)
			c.Shutdown()
			return nil
		})

		// read the message but don't acknowledge it
		res, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.options.GroupName,
			Consumer: "failed_consumer",
			Streams:  []string{tt.Name(), ">"},
			Count:    1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.Len(tt, res[0].Messages, 1)
		require.Equal(tt, msg.ID, res[0].Messages[0].ID)

		// wait for more than VisibilityTimeout + ReclaimInterval to ensure that
		// the pending message is reclaimed
		time.Sleep(6 * time.Millisecond)

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// run the consumer
		c.Run()
	})

	t.Run("doesn't reclaim if there is no VisibilityTimeout set", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			BlockingTimeout: 10 * time.Millisecond,
			ReclaimInterval: 1 * time.Millisecond,
			BufferSize:      100,
			Concurrency:     10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducerWithOptions(ctx, &ProducerOptions{
			StreamMaxLength:      2,
			ApproximateMaxLength: false,
		})
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(ctx, tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(ctx, tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		msg1 := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		msg2 := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value2"},
		}
		err = p.Enqueue(msg1)
		require.NoError(tt, err)

		// register a handler that will assert the message and then shut down
		// the consumer
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, msg2.ID, m.ID)
			c.Shutdown()
			return nil
		})

		// read the message but don't acknowledge it
		res, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.options.GroupName,
			Consumer: "failed_consumer",
			Streams:  []string{tt.Name(), ">"},
			Count:    1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.Len(tt, res[0].Messages, 1)
		require.Equal(tt, msg1.ID, res[0].Messages[0].ID)

		// add another message to the stream to let the consumer consume it
		err = p.Enqueue(msg2)
		require.NoError(tt, err)

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// run the consumer
		c.Run()

		// check if the pending message is still there
		pendingRes, err := c.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: tt.Name(),
			Group:  c.options.GroupName,
			Start:  "-",
			End:    "+",
			Count:  1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, pendingRes, 1)
		require.Equal(tt, msg1.ID, pendingRes[0].ID)
	})

	t.Run("acknowledges pending messages that have already been deleted", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 5 * time.Millisecond,
			BlockingTimeout:   10 * time.Millisecond,
			ReclaimInterval:   1 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducerWithOptions(ctx, &ProducerOptions{
			StreamMaxLength:      1,
			ApproximateMaxLength: false,
		})
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(ctx, tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(ctx, tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		// register a noop handler that should never be called
		c.Register(tt.Name(), func(m *Message) error {
			t.Fail()
			return nil
		})

		// read the message but don't acknowledge it
		res, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.options.GroupName,
			Consumer: "failed_consumer",
			Streams:  []string{tt.Name(), ">"},
			Count:    1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, res, 1)
		require.Len(tt, res[0].Messages, 1)
		require.Equal(tt, msg.ID, res[0].Messages[0].ID)

		// delete the message
		err = c.redis.XDel(ctx, tt.Name(), msg.ID).Err()
		require.NoError(tt, err)

		// watch for consumer errors
		go func() {
			err := <-c.Errors
			require.NoError(tt, err)
		}()

		// in 10ms, shut down the consumer
		go func() {
			time.Sleep(10 * time.Millisecond)
			c.Shutdown()
		}()

		// run the consumer
		c.Run()

		// check that there are no pending messages
		pendingRes, err := c.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: tt.Name(),
			Group:  c.options.GroupName,
			Start:  "-",
			End:    "+",
			Count:  1,
		}).Result()
		require.NoError(tt, err)
		require.Len(tt, pendingRes, 0)
	})

	t.Run("returns an error on a string panic", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer(ctx)
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(ctx, tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(ctx, tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message, shut down the
		// consumer, and then panic with a string
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			c.Shutdown()
			panic("this is a panic")
		})

		// watch for the panic
		go func() {
			err := <-c.Errors
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), "this is a panic")
		}()

		// run the consumer
		c.Run()
	})

	t.Run("returns an error on an error panic", func(tt *testing.T) {
		// create a consumer
		c, err := NewConsumerWithOptions(&ConsumerOptions{
			VisibilityTimeout: 60 * time.Second,
			BlockingTimeout:   10 * time.Millisecond,
			BufferSize:        100,
			Concurrency:       10,
		})
		require.NoError(tt, err)

		// create a producer
		p, err := NewProducer(ctx)
		require.NoError(tt, err)

		// create consumer group
		c.redis.XGroupDestroy(ctx, tt.Name(), c.options.GroupName)
		c.redis.XGroupCreateMkStream(ctx, tt.Name(), c.options.GroupName, "$")

		// enqueue a message
		err = p.Enqueue(&Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		// register a handler that will assert the message, shut down the
		// consumer, and then panic with an error
		c.Register(tt.Name(), func(m *Message) error {
			assert.Equal(tt, "value", m.Values["test"])
			c.Shutdown()
			panic(errors.New("this is a panic"))
		})

		// watch for the panic
		go func() {
			err := <-c.Errors
			require.Error(tt, err)
			assert.Contains(tt, err.Error(), "this is a panic")
		}()

		// run the consumer
		c.Run()
	})
}
