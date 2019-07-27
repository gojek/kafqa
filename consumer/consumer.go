package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gojekfarm/kafqa/callback"
	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	config    config.Consumer
	consumers []consumer
	wg        *sync.WaitGroup
	callbacks []callback.Callback
	exit      chan struct{}
}

type consumer interface {
	SubscribeTopics([]string, kafka.RebalanceCb) error
	ReadMessage(time.Duration) (*kafka.Message, error)
	Close() error
	CommitMessage(*kafka.Message) ([]kafka.TopicPartition, error)
}

func (c *Consumer) Run(ctx context.Context) {
	logger.Debugf("running consumer on brokers: %s, subscribed to: %s", c.config.KafkaBrokers, c.config.Topic)
	for i, cons := range c.consumers {
		c.wg.Add(2)
		msgs := c.consumerWorker(ctx, cons, i) // goroutine producer
		go c.processor(msgs, i)
	}
}

func (c *Consumer) Register(cb callback.Callback) {
	c.callbacks = append(c.callbacks, cb)
}

func (c *Consumer) processor(messages <-chan *kafka.Message, id int) {
	defer c.wg.Done()
	logger.Debugf("[processor-%d] processing messages...", id)
	for msg := range messages {
		for _, cb := range c.callbacks {
			cb(msg)
		}
	}
	logger.Debugf("[processor-%d] completed.", id)
}

func (c *Consumer) consumerWorker(ctx context.Context, cons consumer, id int) <-chan *kafka.Message {
	messages := make(chan *kafka.Message, 1000)

	go func(messages chan *kafka.Message) {
		defer c.wg.Done()
		defer func() { close(messages) }()

		for {
			c.readMessage(cons, messages, id)
			select {
			case <-ctx.Done():
				logger.Debugf("[consumer-%d] context done, closing %v....", id, ctx.Err())
				return
			case <-c.exit:
				return
			default:
				// This is required to preempt goroutine
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(messages)

	return messages
}

func (c *Consumer) readMessage(cons consumer, messages chan<- *kafka.Message, id int) {
	timeout := c.config.PollTimeout()
	logger.Debugf("[consumer-%d] polling kafka for messages... with timeout %v", id, timeout)
	msg, err := cons.ReadMessage(timeout)
	if err != nil {
		kerr, ok := err.(kafka.Error)
		if !(ok && kerr.Code() == kafka.ErrTimedOut) {
			logger.Errorf("error consuming messages: %+v timeout: %v", err, timeout)
		}
	} else {
		messages <- msg
		if !c.config.EnableAutoCommit && msg != nil {
			tps, err := cons.CommitMessage(msg)
			if err != nil {
				logger.Errorf("Error committing message: %v, TopicParitions: %v err: %v", msg, tps, err)
			}
		}
	}

}

func (c *Consumer) Close() {
	logger.Infof("closing consumer...")
	c.exit <- struct{}{}
	c.wg.Wait()
	for _, cons := range c.consumers {
		cons.Close()
	}
}

type Option func(*Consumer)

func WaitGroup(wg *sync.WaitGroup) Option {
	return func(c *Consumer) {
		c.wg = wg
	}
}

func Register(cb callback.Callback) Option {
	return func(c *Consumer) {
		c.callbacks = append(c.callbacks, cb)
	}
}

func New(cfg config.Consumer, opts ...Option) (*Consumer, error) {
	var consumers []consumer
	for i := 0; i < cfg.Concurrency; i++ {
		cons, err := kafka.NewConsumer(cfg.KafkaConfig())
		if err != nil {
			return nil, fmt.Errorf("error creating consumer: %v", err)
		}
		err = cons.SubscribeTopics([]string{cfg.Topic}, nil)
		if err != nil {
			return nil, fmt.Errorf("error subscribing to topic: %v", err)
		}
		consumers = append(consumers, cons)
	}
	cons := &Consumer{
		consumers: consumers,
		config:    cfg,
		exit:      make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(cons)
	}
	return cons, nil
}
