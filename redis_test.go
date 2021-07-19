package redisqueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRedisClient(t *testing.T) {
	t.Run("returns a new redis client", func(tt *testing.T) {
		options := &RedisOptions{}
		r := newRedisClient(options)

		err := r.Ping(context.TODO()).Err()
		assert.NoError(tt, err)
	})

	t.Run("defaults options if it's nil", func(tt *testing.T) {
		r := newRedisClient(nil)

		err := r.Ping(context.TODO()).Err()
		assert.NoError(tt, err)
	})
}

func TestRedisPreflightChecks(t *testing.T) {
	t.Run("bubbles up errors", func(tt *testing.T) {
		options := &RedisOptions{Addr: "localhost:0"}
		r := newRedisClient(options)

		err := redisPreflightChecks(r)
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}
