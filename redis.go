package redisqueue

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

var redisVersionRE = regexp.MustCompile(`redis_version:(.+)`)

// RedisOptions is an alias to redis.Options so that users can this instead of
// having to import go-redis directly.
type RedisOptions = redis.Options

// newRedisClient creates a new Redis client with the given options. If options
// is nil, it will use default options.
func newRedisClient(options *RedisOptions) *redis.Client {
	if options == nil {
		options = &RedisOptions{}
	}
	return redis.NewClient(options)
}

// redisPreflightChecks makes sure the Redis instance backing the *redis.Client
// offers the functionality we need. Specifically, it also that it can connect
// to the actual instance and that the instance supports Redis streams (i.e.
// it's at least v5).
func redisPreflightChecks(client redis.UniversalClient) error {
	_, err := client.Ping().Result()
	if err != nil {
		return err
	}

	return nil
}

// incrementMessageID takes in a message ID (e.g. 1564886140363-0) and
// increments the index section (e.g. 1564886140363-1). This is the next valid
// ID value, and it can be used for paging through messages.
func incrementMessageID(id string) (string, error) {
	parts := strings.Split(id, "-")
	index := parts[1]
	parsed, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return "", errors.Wrapf(err, "error parsing message ID %q", id)
	}
	return fmt.Sprintf("%s-%d", parts[0], parsed+1), nil
}
