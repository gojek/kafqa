package producer

import (
	"fmt"
	"sync"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Handler struct {
	wg     *sync.WaitGroup
	events <-chan kafka.Event
}

func (h Handler) Handle() {
	defer h.wg.Done()

	for e := range h.events {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}

func NewHandler(events <-chan kafka.Event, wg *sync.WaitGroup) Handler {
	return Handler{events: events, wg: wg}
}
