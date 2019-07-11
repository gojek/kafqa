package store

import (
	"fmt"

	"github.com/go-redis/redis"
)

type Redis struct {
	TraceID
	namespace string
	redisdb   *redis.Client
}

func (rs *Redis) Acknowledge(msg Trace) error {
	cmd := rs.redisdb.SAdd(fmt.Sprintf("%s:acked:ids", rs.namespace), rs.TraceID(msg))
	return cmd.Err()
}

func (rs *Redis) Track(msg Trace) error {
	cmd := rs.redisdb.SAdd(fmt.Sprintf("%s:published:ids", rs.namespace), rs.TraceID(msg))
	return cmd.Err()
}

func (rs *Redis) Unacknowledged() ([]string, error) {
	cmd := rs.redisdb.SDiff(fmt.Sprintf("%s:published:ids", rs.namespace), fmt.Sprintf("%s:acked:ids", rs.namespace))
	return cmd.Val(), cmd.Err()
}

func (rs *Redis) Result() Result {
	return Result{}
}

func NewRedis(redisaddr, namespace string, ti TraceID) *Redis {
	return &Redis{
		namespace: namespace,
		redisdb: redis.NewClient(&redis.Options{
			Addr:     redisaddr,
			Password: "",
			DB:       0,
		}),
		TraceID: ti,
	}
}
