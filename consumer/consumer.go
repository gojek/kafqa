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
	consumers []*kafka.Consumer
	wg        *sync.WaitGroup
	callbacks []callback.Callback
	exit      chan struct{}
}

func (c *Consumer) Run(ctx context.Context) {
	logger.Debugf("running consumer on brokers: %s, subscribed to: %s", c.config.KafkaBrokers, c.config.Topic)
	for i, cons := range c.consumers {
		c.wg.Add(2)
		msgs := c.consumerWorker(ctx, cons, i) // goroutine producer
		go c.processor(ctx, msgs, i)
	}
}

func (c *Consumer) Register(cb callback.Callback) {
	c.callbacks = append(c.callbacks, cb)
}

func (c *Consumer) processor(ctx context.Context, messages <-chan *kafka.Message, id int) {
	defer c.wg.Done()
	logger.Debugf("[processor-%d] processing messages...", id)
	for msg := range messages {
		for _, cb := range c.callbacks {
			cb(msg)
		}
	}
	logger.Debugf("[processor-%d] completed.", id)
}

func (c *Consumer) consumerWorker(ctx context.Context, cons *kafka.Consumer, id int) <-chan *kafka.Message {
	messages := make(chan *kafka.Message, 1000)

	go func(messages chan *kafka.Message) {
		defer c.wg.Done()
		defer func() { close(messages) }()

		for {
			logger.Debugf("[consumer-%d] polling kafka for messages... with timeout %v", id, c.config.PollTimeout())
			msg, err := cons.ReadMessage(c.config.PollTimeout())
			if err != nil {
				kerr, ok := err.(kafka.Error)
				if !(ok && kerr.Code() == kafka.ErrTimedOut) {
					logger.Errorf("error consuming messages: %+v timeout: %v", err, c.config.PollTimeout())
				}
			} else {
				messages <- msg
				if !c.config.EnableAutoCommit && msg != nil {
					cons.CommitMessage(msg)
				}
			}
			if err := ctx.Err(); err != nil {
				logger.Debugf("[consumer-%d] context done, closing consumer worker %v", id, err)
				return
			}
			select {
			case <-ctx.Done():
				logger.Debugf("[consumer-%d] context done, closing....", id, err)
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
	var consumers []*kafka.Consumer
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
