package creator_test

import (
	"testing"

	"github.com/gojekfarm/kafqa/creator"
	"github.com/stretchr/testify/assert"
)

func TestNewBytesCreatesMessagesInSequence(t *testing.T) {
	messageCreator := creator.New()
	messageBytes, _ := messageCreator.NewBytes()
	message, err := creator.FromBytes(messageBytes)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), message.Sequence)
	messageBytes, _ = messageCreator.NewBytes()
	message, err = creator.FromBytes(messageBytes)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), message.Sequence)
}
