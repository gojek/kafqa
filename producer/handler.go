package producer

import (
	"sync"

	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/store"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Handler struct {
	wg       *sync.WaitGroup
	events   <-chan kafka.Event
	memStore *store.InMemory
}

func (h *Handler) Handle() {
	defer h.wg.Done()

	for e := range h.events {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				logger.Debugf("Delivery failed: %v", ev.TopicPartition)
			} else {
				msg, err := creator.FromBytes(ev.Value)
				if err != nil {
					logger.Errorf("Decoding Message failed: %v", ev.TopicPartition)
				}
				trace := store.Trace{Message: msg, TopicPartition: ev.TopicPartition}
				err = h.memStore.Track(trace)
				if err != nil {
					logger.Errorf("Couldn't track message: %v", ev.TopicPartition)
				}
			}
		default:
			logger.Debugf("Unknown event type")
		}
	}
}

func NewHandler(events <-chan kafka.Event, wg *sync.WaitGroup, memStore *store.InMemory) *Handler {
	return &Handler{events: events, wg: wg, memStore: memStore}
}
