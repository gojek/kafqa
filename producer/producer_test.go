package producer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestShouldCallRegisteredCallbacks(t *testing.T) {
	logger.Setup("")
	creator := new(msgCreatorMock)
	kafkaProducer := new(kafkaProducerMock)
	kp := Producer{
		kafkaProducer: kafkaProducer,
		config:        config.Producer{Topic: "some_topic", TotalMessages: 1, Concurrency: 1},
		messages:      make(chan []byte, 1000),
		wg:            &sync.WaitGroup{},
		msgCreator:    creator,
	}
	var callbackCalled bool
	ch := make(chan struct{}, 1)
	call := func(msg *kafka.Message) {
		fmt.Println("interesting....")
		callbackCalled = true
		ch <- struct{}{}
	}
	kp.Register(call)
	creator.On("NewBytes").Return([]byte("data1"), nil).Times(1)
	var events chan kafka.Event
	kafkaProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), events).Return(nil).Times(1)
	kafkaProducer.On("Flush", 0).Return(0)
	kafkaProducer.On("Close").Return()

	kp.Run(context.Background())
	<-ch
	kp.Close()

	creator.AssertExpectations(t)
	kafkaProducer.AssertExpectations(t)
	assert.True(t, callbackCalled, "callback should be called")
}

type msgCreatorMock struct{ mock.Mock }

func (m *msgCreatorMock) NewBytes() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

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
