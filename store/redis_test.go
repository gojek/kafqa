package store_test

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type RedisSuite struct {
	suite.Suite
	store      *store.Redis
	messages   []store.Trace
	mr         *miniredis.Miniredis
	testClient *redis.Client
}

func (s *RedisSuite) SetupTest() {
	s.mr, _ = miniredis.Run()
	s.testClient = redis.NewClient(&redis.Options{
		Addr:     s.mr.Addr(),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	msgID := func(t store.Trace) string { return t.Message.ID }
	s.store = store.NewRedis(s.mr.Addr(), "test_namespace", msgID)
	topic := "kafkqa_redis_store"
	tp := kafka.TopicPartition{Topic: &topic, Partition: 1}
	s.messages = []store.Trace{
		{creator.Message{ID: "1"}, tp},
		{creator.Message{ID: "2"}, tp},
		{creator.Message{ID: "3"}, tp},
		{creator.Message{ID: "4"}, tp},
	}
	for _, m := range s.messages {
		s.store.Track(m)
	}
}

func (s *RedisSuite) TeardownTest() {
	s.mr.Close()
}

func (s *RedisSuite) TestShouldAddMessageStatusesToRedisOnTrack() {
	t := s.T()
	cmd := s.testClient.SCard("test_namespace:tracked:ids")
	require.Equal(t, int64(len(s.messages)), cmd.Val())
}

func (s *RedisSuite) TestShouldRedisReturnUnackTrackedMessages() {
	t := s.T()
	pending, err := s.store.Unacknowledged()

	require.NoError(t, err)
	require.Equal(t, len(s.messages), len(pending), "all messages should be in pending list")

	for _, m := range pending {
		assert.Contains(t, []string{"1", "2", "3", "4"}, m)
	}
}

func (s *RedisSuite) TestRedisShouldRemoveAllAcknowledgedMessages() {
	t := s.T()
	for _, m := range s.messages {
		err := s.store.Acknowledge(m)
		require.NoError(t, err)
	}

	pending, err := s.store.Unacknowledged()
	cmd := s.testClient.SCard("test_namespace:acked:ids")
	require.Equal(t, int64(len(s.messages)), cmd.Val())
	require.NoError(t, err)
	require.Empty(t, pending, "pending messages should be empty")
}

func (s *RedisSuite) TestRedisShouldRemoveAcknowledgedMessages() {
	t := s.T()
	err := s.store.Acknowledge(s.messages[0])
	require.NoError(t, err)
	err = s.store.Acknowledge(s.messages[2])
	require.NoError(t, err)

	pending, err := s.store.Unacknowledged()
	cmd := s.testClient.SCard("test_namespace:acked:ids")
	require.Equal(t, int64(2), cmd.Val())
	require.NoError(t, err)
	require.Equal(t, 2, len(pending), "pending messages should have 2 non ack messages")
	for _, m := range pending {
		assert.Contains(t, []string{"2", "4"}, m)
	}
}

func (s *RedisSuite) TestFetchFromRedisShouldBeSourceForResult() {
	t := s.T()
	result := s.store.Result()
	require.Equal(t, int64(4), result.Tracked)
	require.Equal(t, int64(0), result.Acknowledged)
}

func TestRedisStore(t *testing.T) {
	suite.Run(t, new(RedisSuite))
}
