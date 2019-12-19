package store_test

import (
	"testing"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/store"
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

func TestShouldCheckIfInMemoryStoreIsReturned(t *testing.T) {
	appConfig := config.Application{Producer: config.Producer{TotalMessages: 100, Enabled: true}, Consumer: config.Consumer{Enabled: true}}
	traceID := func(t store.Trace) string { return t.Message.ID }

	newStore, err := store.New(appConfig, traceID)
	storeType, ok := newStore.(*store.InMemory)

	assert.True(t, ok)
	require.NoError(t, err)
	require.NotEmptyf(t, storeType, "")
}

func TestShouldCheckIfNoOpStoreIsReturned(t *testing.T) {
	appConfig := config.Application{Producer: config.Producer{TotalMessages: -1, Enabled: true}}
	traceID := func(t store.Trace) string { return t.Message.ID }

	newStore, err := store.New(appConfig, traceID)
	storeType, ok := newStore.(store.NoOp)

	assert.True(t, ok)
	require.NoError(t, err)
	require.Empty(t, storeType, "")
}
