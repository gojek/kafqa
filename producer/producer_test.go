package producer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gojekfarm/kafqa/serde"

	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"

	"github.com/gojekfarm/kafqa/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type ProducerSuite struct {
	suite.Suite
	kafkaProducer *kafkaProducerMock
	creator       *msgCreatorMock
	encoder       serde.Encoder
	kp            Producer
}

func (s *ProducerSuite) SetupTest() {
	logger.Setup("none")

	s.kafkaProducer = new(kafkaProducerMock)
	s.creator = new(msgCreatorMock)
	s.kp = Producer{
		kafkaProducer: s.kafkaProducer,
		msgCreator:    s.creator,
		encoder:       serde.KafqaParser{},
		wg:            &sync.WaitGroup{},
		messages:      make(chan creator.Message, 1000),
	}
	s.encoder = serde.KafqaParser{}
}

func (s *ProducerSuite) TestShouldCallRegisteredCallbacks() {
	t := s.T()
	var callbackCalled bool
	ch := make(chan struct{}, 1)
	callback := func(msg *kafka.Message) {
		callbackCalled = true
		ch <- struct{}{}
	}
	prodCh := make(chan *kafka.Message)
	s.kp.messages = make(chan creator.Message, 1)
	s.encoder = serde.KafqaParser{}
	s.kp.config = config.Producer{TotalMessages: 1, Concurrency: 1, Topic: "sometopic"}
	opt := Register(callback)
	opt(&s.kp)
	s.creator.On("NewMessageWithFakeData").Return(creator.Message{}, nil).Times(1)
	var events chan kafka.Event
	s.kafkaProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), events).Return(nil).Times(1)
	s.kafkaProducer.On("Flush", 0).Return(0)
	s.kafkaProducer.On("Close").Return()
	s.kafkaProducer.On("ProduceChannel").Return(prodCh).Maybe()
	s.kp.Run(context.Background())
	<-ch
	s.kp.Close()

	s.creator.AssertExpectations(t)
	s.kafkaProducer.AssertExpectations(t)
	assert.True(t, callbackCalled, "callback should be called")
}

func (s *ProducerSuite) TestIfAllMessagesAreProduced() {
	t := s.T()
	msg := make(chan struct{}, 1000)
	var events chan kafka.Event
	callback := func(message *kafka.Message) {
		msg <- struct{}{}
	}
	prodCh := make(chan *kafka.Message)
	s.encoder = serde.KafqaParser{}
	s.kp.config = config.Producer{TotalMessages: 1000, Concurrency: 10, Topic: "sometopic"}
	opt := Register(callback)
	opt(&s.kp)
	s.kafkaProducer.On("Produce", mock.AnythingOfTypeArgument("*kafka.Message"), events).Return(nil)
	s.kafkaProducer.On("Close").Return()
	s.kafkaProducer.On("Flush", 0).Return(0)
	s.kafkaProducer.On("ProduceChannel").Return(prodCh).Maybe()

	s.creator.On("NewMessageWithFakeData").Return(creator.Message{}, nil)

	s.kp.Run(context.Background())
	for i := 0; i < 1000; i++ {
		<-msg
	}
	s.kp.Close()
	s.kafkaProducer.AssertNumberOfCalls(t, "Produce", 1000)
	s.kafkaProducer.AssertExpectations(t)
	s.creator.AssertExpectations(t)
}

func (s *ProducerSuite) TestIfMessagesAreProducedInfinitely() {
	t := s.T()
	var events chan kafka.Event
	msgs := int32(0)
	callback := func(message *kafka.Message) {
		atomic.AddInt32(&msgs, int32(1))
	}
	prodCh := make(chan *kafka.Message)
	s.encoder = serde.KafqaParser{}
	s.kp.config = config.Producer{TotalMessages: -1, Concurrency: 10, Topic: "sometopic"}
	opt := Register(callback)
	opt(&s.kp)
	s.kafkaProducer.On("Produce", mock.AnythingOfTypeArgument("*kafka.Message"), events).Return(nil)
	s.kafkaProducer.On("Close").Return()
	s.kafkaProducer.On("Flush", 0).Return(0)
	s.kafkaProducer.On("ProduceChannel").Return(prodCh).Maybe()

	s.creator.On("NewMessageWithFakeData").Return(creator.Message{}, nil)

	d := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	s.kp.Run(ctx)

	time.AfterFunc(time.Second*5, func() { s.T().Fatalf("Producer didn't complete within expected time") })
	s.kp.Close()
	assert.GreaterOrEqual(t, int32(len(s.kafkaProducer.Calls)), msgs)
	s.kafkaProducer.AssertExpectations(t)
	s.creator.AssertExpectations(t)
}

func TestProducer(t *testing.T) {
	suite.Run(t, new(ProducerSuite))
}

type msgCreatorMock struct{ mock.Mock }

func (m *msgCreatorMock) NewMessageWithFakeData() creator.Message {
	args := m.Called()
	return args.Get(0).(creator.Message)
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

func (m *kafkaProducerMock) ProduceChannel() chan *kafka.Message {
	args := m.Called()
	return args.Get(0).(chan *kafka.Message)
}
