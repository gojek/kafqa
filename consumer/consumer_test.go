package consumer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gojek/kafqa/config"
	"github.com/gojek/kafqa/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type ConsumerSuite struct {
	suite.Suite
	consumer *Consumer
}

func (s *ConsumerSuite) SetupTest() {
	logger.Setup("")
	var consumers []consumer
	for i := 0; i < 1; i++ {
		kafkaconsumer := new(consumerMock)
		msg := &kafka.Message{}
		kafkaconsumer.On("Close").Return(nil)
		kafkaconsumer.On("ReadMessage", mock.AnythingOfType("time.Duration")).Return(msg, nil)
		kafkaconsumer.On("CommitMessage", msg).Return(make([]kafka.TopicPartition, 1), nil)
		consumers = append(consumers, kafkaconsumer)
	}
	s.consumer = &Consumer{
		config:    config.Consumer{Concurrency: 1},
		consumers: consumers,
		wg:        &sync.WaitGroup{},
		cbwg:      &sync.WaitGroup{},
		exit:      make(chan struct{}, 1),
	}
}

func (s *ConsumerSuite) TestIfCallbackCalled() {
	t := s.T()
	ch := make(chan struct{}, 1)
	var callbackCalled int32
	call := func(msg *kafka.Message) {
		go func() {
			atomic.AddInt32(&callbackCalled, int32(1))
			ch <- struct{}{}
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	opt := Register(call)
	opt(s.consumer)
	s.consumer.Run(ctx)
	<-ch
	cancel()
	s.consumer.Close()

	assert.GreaterOrEqual(t, atomic.LoadInt32(&callbackCalled), int32(1), "Callback called! Message received")
}

func TestConsumer(t *testing.T) {
	suite.Run(t, new(ConsumerSuite))
}

type consumerMock struct {
	mock.Mock
}

func (c *consumerMock) SubscribeTopics(topics []string, rebalance kafka.RebalanceCb) error {
	args := c.Called(topics, rebalance)
	return args.Error(0)
}

func (c *consumerMock) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	args := c.Called(timeout)
	return args.Get(0).(*kafka.Message), args.Error(1)
}

func (c *consumerMock) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *consumerMock) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	args := c.Called(msg)
	return args.Get(0).([]kafka.TopicPartition), args.Error(1)
}
