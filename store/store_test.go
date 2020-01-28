package store_test

import (
	"reflect"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/gojek/kafqa/config"
	"github.com/gojek/kafqa/creator"
	"github.com/gojek/kafqa/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type InmemorySuite struct {
	suite.Suite
	store    *store.InMemory
	messages []store.Trace
}

func (s *InmemorySuite) SetupTest() {
	msgID := func(t store.Trace) string { return t.Message.ID }
	s.store = store.NewInMemory(msgID)
	topic := "kafkqa_mem_store"
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

func (s *InmemorySuite) ShouldReturnUnackTrackedMessages() {
	t := s.T()
	pending, err := s.store.Unacknowledged()

	require.NoError(t, err)
	require.Equal(t, len(s.messages), len(pending), "all messages should be in pending list")

	for _, m := range pending {
		assert.Contains(t, []string{"1", "2", "3", "4"}, m)
	}
}

func (s *InmemorySuite) TestShouldRemoveAllAcknowledgedMessages() {
	t := s.T()
	for _, m := range s.messages {
		err := s.store.Acknowledge(m)
		require.NoError(t, err)
	}

	pending, err := s.store.Unacknowledged()

	require.NoError(t, err)
	require.Empty(t, pending, "pending messages should be empty")
}

func (s *InmemorySuite) TestShouldRemoveAcknowledgedMessages() {
	t := s.T()
	err := s.store.Acknowledge(s.messages[0])
	require.NoError(t, err)
	err = s.store.Acknowledge(s.messages[2])
	require.NoError(t, err)

	pending, err := s.store.Unacknowledged()

	require.NoError(t, err)
	require.Equal(t, 2, len(pending), "pending messages should have 2 non ack messages")
	for _, m := range pending {
		assert.Contains(t, []string{"2", "4"}, m)
	}
}

func TestValidatesStoreCreated(t *testing.T) {
	producer := config.Producer{TotalMessages: 100, Enabled: true}
	consumer := config.Consumer{Enabled: true}
	traceID := func(t store.Trace) string { return t.Message.ID }
	mr, _ := miniredis.Run()

	testCases := []struct {
		expectedType string
		appConfig    config.Application
	}{
		{
			"*store.Redis",
			config.Application{
				Producer: producer,
				Consumer: consumer,
				Store:    config.Store{Type: "redis", RunID: "test_namespace", RedisHost: mr.Addr()},
			},
		},
		{
			"*store.InMemory",
			config.Application{
				Producer: producer,
				Consumer: consumer,
			},
		},
		{
			"store.NoOp",
			config.Application{
				Producer: config.Producer{TotalMessages: -1},
				Consumer: consumer,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.expectedType, func(t *testing.T) {
			newStore, err := store.New(testCase.appConfig, traceID)
			require.Nil(t, err)
			actualType := reflect.TypeOf(newStore).String()
			assert.Equal(t, testCase.expectedType, actualType)
		})
	}
}
