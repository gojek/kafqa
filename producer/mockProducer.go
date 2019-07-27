package producer

import (
	"github.com/stretchr/testify/mock"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type kafkaProducerMock struct{ mock.Mock }

func (m *kafkaProducerMock) Produce(msg *kafka.Message, events chan kafka.Event) error {
	args := m.Called(msg, events)
	return args.Error(0)
}

func (m *kafkaProducerMock) Flush(t int) int {
	return m.Called(t).Int(0)
}

func (m *kafkaProducerMock) Events() chan kafka.Event {
	return m.Called().Get(0).(chan kafka.Event)
}

func (m *kafkaProducerMock) Close() { m.Called() }
