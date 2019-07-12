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

func (rs *Redis) keyFor(kind string) string {
	return fmt.Sprintf("%s:%s:ids", rs.namespace, kind)
}

func (rs *Redis) Acknowledge(msg Trace) error {
	cmd := rs.redisdb.SAdd(rs.keyFor("acked"), rs.TraceID(msg))
	return cmd.Err()
}

func (rs *Redis) Track(msg Trace) error {
	cmd := rs.redisdb.SAdd(rs.keyFor("tracked"), rs.TraceID(msg))
	return cmd.Err()
}

func (rs *Redis) Unacknowledged() ([]string, error) {
	cmd := rs.redisdb.SDiff(rs.keyFor("tracked"), rs.keyFor("acked"))
	return cmd.Val(), cmd.Err()
}

func (rs *Redis) Result() Result {
	cmd := rs.redisdb.SCard(rs.keyFor("tracked"))
	if cmd.Err() != nil {
		return Result{}
	}
	numTracked := cmd.Val()
	cmd = rs.redisdb.SCard(rs.keyFor("acked"))
	if cmd.Err() != nil {
		return Result{}
	}
	numAcked := cmd.Val()
	return Result{Tracked: numTracked, Acknowledged: numAcked}
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
