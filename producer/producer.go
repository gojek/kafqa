package producer

import (
	"fmt"

	"github.com/gojekfarm/kafqa/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Producer struct {
	Topic string
	*kafka.Producer
}

func (p Producer) Run() {

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := p.Topic
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}

func New(prodCfg config.Producer) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": prodCfg.KafkaBrokers})
	if err != nil {
		return nil, err
	}
	return &Producer{prodCfg.Topic, p}, nil
}
