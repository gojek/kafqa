package consumer

import (
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/store"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Callback func(*kafka.Message)

func Display(msg *kafka.Message) {
	message, _ := creator.FromBytes(msg.Value)
	logger.Debugf("received message on %s: message: %s\n", msg.TopicPartition, message)
}

type acknowledger interface {
	Acknowledge(store.Trace) error
}

func Acker(ack acknowledger) Callback {
	return func(msg *kafka.Message) {
		message, err := creator.FromBytes(msg.Value)
		if err != nil {
			logger.Debugf("Unable to decode message during consumer ack")
		} else {
			err := ack.Acknowledge(store.Trace{Message: message, TopicPartition: msg.TopicPartition})
			if err != nil {
				logger.Debugf("Unable to acknowledge message: %s", message)
			}
		}
	}
}
