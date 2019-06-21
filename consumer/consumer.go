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
}

func (c *Consumer) Run(ctx context.Context) {
	c.Add(1)
	logger.Debugf("running consumer on brokers: %s, subscribed to: %s", c.config.KafkaBrokers, c.config.Topic)
	go c.consume(ctx)
	c.Done()
}

func (c *Consumer) consume(ctx context.Context) {
	for {
		select {
		case event := <-c.Events():
			c.processEvent(event)
		case <-ctx.Done():
			logger.Debugf("context done, closing consumer")
			return
		}
	}
}
func (c *Consumer) processEvent(event kafka.Event) {
	switch ev := event.(type) {
	case *kafka.Message:
		msg := ev
		logger.Debugf("received message on %s: message: %s\n", msg.TopicPartition, string(msg.Value))
	default:
		logger.Debugf("received event: %v", ev)
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

	return &Consumer{Consumer: cons, config: cfg}, nil
}
