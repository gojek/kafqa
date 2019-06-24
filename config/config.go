package config

import (
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Application struct {
	Producer
	Consumer
	Log
}

type Log struct {
	Level string `split_words:"true" default:"info"`
}

type Producer struct {
	Topic          string `default:"kafqa_test" envconfig:"KAFKA_TOPIC"`
	Concurrency    int    `default:"100"`
	TotalMessages  uint64 `split_words:"true" default:"10000"`
	KafkaBrokers   string `split_words:"true" required:"true"`
	FlushTimeoutMs int    `split_words:"true" default:"2000"`
}

type Consumer struct {
	Topic         string `default:"kafqa_test" envconfig:"KAFKA_TOPIC"`
	Concurrency   int    `default:"20"`
	KafkaBrokers  string `split_words:"true" required:"true"`
	GroupID       string `split_words:"true" default:"kafqa_test_consumer_001"`
	OffsetReset   string `split_words:"true" default:"earliest"`
	PollTimeoutMs int64  `split_words:"true" default:"-1"`
}

func App() Application {
	return application
}

func (c Consumer) KafkaConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		KafkaBootstrapServerKey: c.KafkaBrokers,
		ConsumerOffsetResetKey:  c.OffsetReset,
		ConsumerGroupIDKey:      c.GroupID,
	}
}

func (c Consumer) PollTimeout() time.Duration {
	return time.Duration(c.PollTimeoutMs) * time.Millisecond
}
