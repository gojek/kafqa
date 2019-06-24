package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/gojekfarm/kafqa/config"
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
	c.Add(1)
	logger.Debugf("running consumer on brokers: %s, subscribed to: %s", c.config.KafkaBrokers, c.config.Topic)
	go c.processor(ctx)
	go c.consumerWorker(ctx)
	c.Done()
}

func (c *Consumer) processor(ctx context.Context) {
	for {
		select {
		case msg := <-c.messages:
			logger.Debugf("received message on %s: message: %s\n", msg.TopicPartition, string(msg.Value))
		case <-ctx.Done():
			logger.Debugf("context done, closing consumer")
			return
		}
	}
}

func (c *Consumer) consumerWorker(ctx context.Context) {
	for {
		msg, err := c.ReadMessage(c.config.PollTimeout())
		if err != nil {
			logger.Errorf("error consuming messages: %v timeout: %v", err, c.config.PollTimeout())
		} else {
			c.messages <- msg
		}
		if err := ctx.Err(); err != nil {
			logger.Debugf("context done, closing consumer worker %v", err)
			return
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
