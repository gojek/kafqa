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
	sync.WaitGroup
	messages  chan *kafka.Message
	callbacks []callback.Callback
}

func (c *Consumer) Run(ctx context.Context) {
	c.Add(c.config.Concurrency * 2)
	logger.Debugf("running consumer on brokers: %s, subscribed to: %s", c.config.KafkaBrokers, c.config.Topic)
	for i, cons := range c.consumers {
		go c.consumerWorker(ctx, cons, i)
		go c.processor(ctx)
	}
}

func (c *Consumer) Register(cb callback.Callback) {
	c.callbacks = append(c.callbacks, cb)
}

func (c *Consumer) processor(ctx context.Context) {
	defer c.Done()
	for {
		select {
		case msg := <-c.messages:
			for _, cb := range c.callbacks {
				cb(msg)
			}
		case <-ctx.Done():
			logger.Debugf("context done, closing consumer")
			return
		}
	}
}

func (c *Consumer) consumerWorker(ctx context.Context, cons *kafka.Consumer, id int) {
	defer c.Done()

	for {
		logger.Debugf("[consumer-%d] polling kafka for messages... with timeout %v", id, c.config.PollTimeout())
		msg, err := cons.ReadMessage(c.config.PollTimeout())
		if err != nil {
			kerr, ok := err.(kafka.Error)
			if ok && kerr.Code() != kafka.ErrTimedOut {
				logger.Errorf("error consuming messages: %+v timeout: %v", kerr, c.config.PollTimeout())
			} else {
				logger.Errorf("error consuming messages: %+v timeout: %v", err, c.config.PollTimeout())
			}
		} else {
			c.messages <- msg
		}
		if err := ctx.Err(); err != nil {
			logger.Debugf("context done, closing consumer worker %v", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
			// This is required to preempt goroutine
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (c *Consumer) Close() {
	logger.Infof("closing consumer...")
	c.Wait()
	for _, cons := range c.consumers {
		cons.Close()
	}
}

func New(cfg config.Consumer) (*Consumer, error) {
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

	return &Consumer{
		consumers: consumers,
		config:    cfg,
		messages:  make(chan *kafka.Message, 100),
	}, nil
}
