package creator_test

import (
	"strings"
	"testing"
	"time"

	"github.com/gojekfarm/kafqa/creator"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewBytesCreatesMessagesInSequence(t *testing.T) {
	messageCreator := creator.New()
	messageBytes, _ := messageCreator.NewMessage().Bytes()
	message, err := creator.FromBytes(messageBytes)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), message.Sequence)
	messageBytes, _ = messageCreator.NewMessage().Bytes()
	message, err = creator.FromBytes(messageBytes)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), message.Sequence)
}

func TestAddsUUIDV4AsID(t *testing.T) {
	messageCreator := creator.New()
	messageBytes, _ := messageCreator.NewMessage().Bytes()
	message, err := creator.FromBytes(messageBytes)
	uid, err := uuid.FromString(message.ID)
	assert.NoError(t, err)
	assert.Equal(t, uuid.V4, uid.Version())
}

func TestAddsCreationTimeStamp(t *testing.T) {
	messageCreator := creator.New()
	messageBytes, _ := messageCreator.NewMessage().Bytes()
	message, err := creator.FromBytes(messageBytes)
	assert.NoError(t, err)

	createdSince := time.Since(message.CreatedTime)
	assert.Equal(t, time.Duration(0*time.Second), createdSince.Round(time.Second))
}

func TestAdds10ParasOfText(t *testing.T) {
	messageCreator := creator.New()
	messageBytes, _ := messageCreator.NewMessage().Bytes()
	message, err := creator.FromBytes(messageBytes)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(strings.Split(string(message.Data), "\t")))
}
