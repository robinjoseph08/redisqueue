package redisqueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	t.Run("creates a new producer", func(tt *testing.T) {
		p, err := NewProducer(ctx)
		require.NoError(tt, err)

		assert.NotNil(tt, p)
	})
}

func TestNewProducerWithOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("creates a new producer", func(tt *testing.T) {
		p, err := NewProducerWithOptions(ctx, &ProducerOptions{})
		require.NoError(tt, err)

		assert.NotNil(tt, p)
	})

	t.Run("allows custom *redis.Client", func(tt *testing.T) {
		rc := newRedisClient(nil)

		p, err := NewProducerWithOptions(ctx, &ProducerOptions{
			RedisClient: rc,
		})
		require.NoError(tt, err)

		assert.NotNil(tt, p)
		assert.Equal(tt, rc, p.redis)
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		_, err := NewProducerWithOptions(ctx, &ProducerOptions{
			RedisOptions: &RedisOptions{Addr: "localhost:0"},
		})
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}

func TestEnqueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("puts the message in the stream", func(tt *testing.T) {
		p, err := NewProducerWithOptions(ctx, &ProducerOptions{})
		require.NoError(t, err)

		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(msg)
		require.NoError(tt, err)

		res, err := p.redis.XRange(ctx, msg.Stream, msg.ID, msg.ID).Result()
		require.NoError(tt, err)
		assert.Equal(tt, "value", res[0].Values["test"])
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		p, err := NewProducerWithOptions(ctx, &ProducerOptions{ApproximateMaxLength: true})
		require.NoError(t, err)

		msg := &Message{
			Stream: tt.Name(),
		}
		err = p.Enqueue(msg)
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "wrong number of arguments")
	})
}
