package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	config config.Consumer
	*kafka.Consumer
	sync.WaitGroup
	messages chan *kafka.Message
}

func (c *Consumer) Run(ctx context.Context) {
	c.Add(2)
	logger.Debugf("running consumer on brokers: %s, subscribed to: %s", c.config.KafkaBrokers, c.config.Topic)
	go c.processor(ctx)
	go c.consumerWorker(ctx)
}

func (c *Consumer) processor(ctx context.Context) {
	defer c.Done()
	for {
		select {
		case msg := <-c.messages:
			message, _ := creator.FromBytes(msg.Value)
			logger.Debugf("received message on %s: message: %s\n", msg.TopicPartition, message)
		case <-ctx.Done():
			logger.Debugf("context done, closing consumer")
			return
		}
	}
}

func (c *Consumer) consumerWorker(ctx context.Context) {
	defer c.Done()

	for {
		logger.Debugf("polling kafka for messages... with timeout %v", c.config.PollTimeout())
		msg, err := c.ReadMessage(c.config.PollTimeout())
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
	c.Consumer.Close()
}

func New(cfg config.Consumer) (*Consumer, error) {
	cons, err := kafka.NewConsumer(cfg.KafkaConfig())
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}
	err = cons.SubscribeTopics([]string{cfg.Topic}, nil)
	if err != nil {
		return nil, fmt.Errorf("error subscribing to topic: %v", err)
	}

	return &Consumer{
		Consumer: cons,
		config:   cfg,
		messages: make(chan *kafka.Message, 100),
	}, nil
}
