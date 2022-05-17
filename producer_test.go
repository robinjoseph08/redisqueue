package redisqueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProducer(t *testing.T) {
	t.Run("creates a new producer", func(tt *testing.T) {
		p, err := NewProducer(context.Background())
		require.NoError(tt, err)

		assert.NotNil(tt, p)
	})
}

func TestNewProducerWithOptions(t *testing.T) {
	t.Run("creates a new producer", func(tt *testing.T) {
		p, err := NewProducerWithOptions(context.Background(), &ProducerOptions{})
		require.NoError(tt, err)

		assert.NotNil(tt, p)
	})

	t.Run("allows custom *redis.Client", func(tt *testing.T) {
		rc := newRedisClient(nil)

		p, err := NewProducerWithOptions(context.Background(), &ProducerOptions{
			RedisClient: rc,
		})
		require.NoError(tt, err)

		assert.NotNil(tt, p)
		assert.Equal(tt, rc, p.redis)
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		_, err := NewProducerWithOptions(context.Background(), &ProducerOptions{
			RedisOptions: &RedisOptions{Addr: "localhost:0"},
		})
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}

func TestEnqueue(t *testing.T) {
	t.Run("puts the message in the stream", func(tt *testing.T) {
		p, err := NewProducerWithOptions(context.Background(), &ProducerOptions{})
		require.NoError(t, err)

		msg := &Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		}
		err = p.Enqueue(context.Background(), msg)
		require.NoError(tt, err)

		res, err := p.redis.XRange(context.Background(), msg.Stream, msg.ID, msg.ID).Result()
		require.NoError(tt, err)
		assert.Equal(tt, "value", res[0].Values["test"])
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		p, err := NewProducerWithOptions(context.Background(), &ProducerOptions{ApproximateMaxLength: true})
		require.NoError(t, err)

		msg := &Message{
			Stream: tt.Name(),
		}
		err = p.Enqueue(context.Background(), msg)
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "wrong number of arguments")
	})
}
