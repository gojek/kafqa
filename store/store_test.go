package store_test

import (
	"testing"

	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type InmemorySuite struct {
	suite.Suite
	store    *store.InMemory
	messages []creator.Message
}

func (s *InmemorySuite) TestSetup() {
	s.store = store.NewInMemory()
	s.messages = []creator.Message{
		{ID: "1"},
		{ID: "2"},
		{ID: "3"},
		{ID: "4"},
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
		assert.Contains(t, []string{"1", "2", "3", "4"}, m.ID)
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
		assert.Contains(t, []string{"2", "4"}, m.ID)
	}
}

func TestInMemoryStore(t *testing.T) {
	suite.Run(t, new(InmemorySuite))
}
