package serde

import (
	"bytes"
	"encoding/gob"

	"github.com/gojekfarm/kafqa/creator"
)

type KafqaParser struct {
}

// Bytes Serializes the message via gob encoder
func (kafqaParser KafqaParser) Bytes(m creator.Message) ([]byte, error) {
	var messageBuffer bytes.Buffer
	enc := gob.NewEncoder(&messageBuffer)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return messageBuffer.Bytes(), nil
}

// FromBytes DeSerializes the bytes data to a message via gob decoder
func (kafqaParser KafqaParser) FromBytes(data []byte) (creator.Message, error) {
	var messageBuffer bytes.Buffer
	messageBuffer.Write(data)
	enc := gob.NewDecoder(&messageBuffer)
	var m creator.Message
	err := enc.Decode(&m)
	return m, err
}
