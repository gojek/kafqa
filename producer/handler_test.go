package producer

import (
	"context"
	"sync"
	"testing"

	"github.com/gojekfarm/kafqa/logger"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/store"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type HandlerSuite struct {
	suite.Suite
	kafkaProducer *kafkaProducerMock
	msgCreator    *msgCreatorMock
	msgStore      *InMemoryStoreMock
	kp            Producer
}

func (s *HandlerSuite) SetupTest() {
	logger.Setup("")
	s.kafkaProducer = new(kafkaProducerMock)
	s.msgCreator = new(msgCreatorMock)
	s.kp = Producer{
		kafkaProducer: s.kafkaProducer,
		msgCreator:    s.msgCreator,
		config:        config.Producer{Topic: "sometopic", TotalMessages: 1000, Concurrency: 10},
		wg:            &sync.WaitGroup{},
		messages:      make(chan []byte, 1000),
	}
	s.msgStore = new(InMemoryStoreMock)
}

func (s *HandlerSuite) TestIfAllMsgsAreTracedIfEventIsKafkaMsg() {
	t := s.T()
	ch := make(chan struct{}, 1000)
	callback := func(*kafka.Message) {
		ch <- struct{}{}
	}
	s.kp.Register(callback)
	wg := &sync.WaitGroup{}
	eventsCh := make(chan kafka.Event, 1000)
	deliveryHandler := Handler{
		wg:       wg,
		events:   eventsCh,
		msgStore: s.msgStore,
	}
	msg, _ := (&creator.Creator{}).NewBytes()
	topic := "topic1"
	kafkaMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          msg,
	}
	prodCh := make(chan *kafka.Message)
	var events chan kafka.Event
	s.kafkaProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), events).Return(nil).Run(func(args mock.Arguments) {
		eventsCh <- &kafkaMessage
	})
	s.kafkaProducer.On("Close").Return()
	s.kafkaProducer.On("Flush", 0).Return(0)
	s.kafkaProducer.On("ProduceChannel").Return(prodCh).Maybe()
	s.msgCreator.On("NewBytes").Return([]byte("somedata"), nil)
	s.msgStore.On("Track", mock.AnythingOfType("store.Trace")).Return(nil)

	wg.Add(1)
	s.kp.Run(context.Background())
	go deliveryHandler.Handle()
	for i := 0; i < 1000; i++ {
		<-ch
	}
	s.kp.Close()
	close(eventsCh)
	wg.Wait()
	s.msgStore.AssertNumberOfCalls(t, "Track", 1000)
	s.kafkaProducer.AssertExpectations(t)
	s.msgStore.AssertExpectations(t)
}

func (s *HandlerSuite) TestIfNoMsgsAreTracedIfEventTypeIsUnknown() {
	t := s.T()
	eventsCh := make(chan kafka.Event, 1000)
	ch := make(chan struct{}, 1000)
	callback := func(*kafka.Message) {
		ch <- struct{}{}
	}
	s.kp.Register(callback)
	wg := &sync.WaitGroup{}
	deliveryHandler := Handler{
		wg:       wg,
		events:   eventsCh,
		msgStore: s.msgStore,
	}
	prodCh := make(chan *kafka.Message)
	var events chan kafka.Event
	s.kafkaProducer.On("Produce", mock.AnythingOfType("*kafka.Message"), events).Return(nil).Run(func(args mock.Arguments) {
		eventsCh <- new(kafka.Error)
	})
	s.kafkaProducer.On("Close").Return()
	s.kafkaProducer.On("Flush", 0).Return(0)
	s.kafkaProducer.On("ProduceChannel").Return(prodCh).Maybe()

	s.msgCreator.On("NewBytes").Return([]byte("somedata"), nil)
	wg.Add(1)
	s.kp.Run(context.Background())
	go deliveryHandler.Handle()
	for i := 0; i < 1000; i++ {
		<-ch
	}
	s.kp.Close()
	close(eventsCh)
	wg.Wait()
	s.msgStore.AssertNumberOfCalls(t, "Track", 0)
	s.kafkaProducer.AssertExpectations(t)
	s.msgCreator.AssertExpectations(t)
}

func TestHandler(t *testing.T) {
	suite.Run(t, new(HandlerSuite))
}

type InMemoryStoreMock struct {
	mock.Mock
}

func (m *InMemoryStoreMock) Acknowledge(msg store.Trace) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *InMemoryStoreMock) Track(msg store.Trace) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *InMemoryStoreMock) Unacknowledged() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *InMemoryStoreMock) Result() store.Result {
	args := m.Called()
	return args.Get(0).(store.Result)
}
