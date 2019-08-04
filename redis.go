package redisqueue

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

var redisVersionRE = regexp.MustCompile(`redis_version:(.+)`)

// RedisOptions is an alias to redis.Options so that users can this instead of
// having to import go-redis directly.
type RedisOptions = redis.Options

// newRedisClient creates a new Redis client with the given options. If options
// is nil, it will use default options. In addition to creating the client, it
// also ensures that it can connect to the actual instance and that the instance
// supports Redis streams (i.e. it's at least v5).
func newRedisClient(options *RedisOptions) (*redis.Client, error) {
	if options == nil {
		options = &RedisOptions{}
	}
	client := redis.NewClient(options)

	// make sure Redis supports streams (i.e. is at least v5)
	info, err := client.Info("server").Result()
	if err != nil {
		return nil, err
	}

	match := redisVersionRE.FindAllStringSubmatch(info, -1)
	if len(match) < 1 {
		return nil, fmt.Errorf("could not extract redis version")
	}
	version := strings.TrimSpace(match[0][1])
	parts := strings.Split(version, ".")
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	if major < 5 {
		return nil, fmt.Errorf("redis streams are not supported in version %q", version)
	}

	return client, nil
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
