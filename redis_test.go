package redisqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRedisClient(t *testing.T) {
	t.Run("returns a new redis client", func(tt *testing.T) {
		options := &RedisOptions{}
		r, err := newRedisClient(options)
		require.NoError(tt, err)

		err = r.Ping().Err()
		assert.NoError(tt, err)
	})

	t.Run("defaults options if it's nil", func(tt *testing.T) {
		r, err := newRedisClient(nil)
		require.NoError(tt, err)

		err = r.Ping().Err()
		assert.NoError(tt, err)
	})

	t.Run("bubbles up errors", func(tt *testing.T) {
		options := &RedisOptions{Addr: "localhost:0"}
		_, err := newRedisClient(options)
		require.Error(tt, err)

		assert.Contains(tt, err.Error(), "dial tcp")
	})
}
