package creator_test

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/gojekfarm/kafqa/creator"
	"github.com/stretchr/testify/assert"
)

func TestBytesShouldSerializeInGobEncoding(t *testing.T) {
	var messageBuffer bytes.Buffer
	message := creator.Message{Sequence: uint64(1010)}
	messageBytes, _ := message.Bytes()
	messageBuffer.Write(messageBytes)
	decoder := gob.NewDecoder(&messageBuffer)
	var decodedMessage creator.Message
	err := decoder.Decode(&decodedMessage)
	assert.NoError(t, err)
	assert.Equal(t, message, decodedMessage)
}

func TestBytesShouldDeSerializeInGobEncoding(t *testing.T) {
	message := creator.Message{Sequence: uint64(1011)}
	mBytes, _ := message.Bytes()
	deserializedMessage, err := creator.FromBytes(mBytes)
	assert.NoError(t, err)
	assert.Equal(t, message, deserializedMessage)
}
