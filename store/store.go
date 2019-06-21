package store

import (
	"sync"

	"github.com/gojekfarm/kafqa/creator"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Trace struct {
	creator.Message
	kafka.TopicPartition
}

type TraceID func(Trace) string

type InMemory struct {
	pending map[string]Trace
	sync.Mutex
	TraceID
}

func (ms *InMemory) Acknowledge(msg Trace) error {
	ms.Lock()
	defer ms.Unlock()

	delete(ms.pending, ms.TraceID(msg))
	return nil
}

func (ms *InMemory) Track(msg Trace) error {
	ms.Lock()
	defer ms.Unlock()

	ms.pending[ms.TraceID(msg)] = msg
	return nil
}

func (ms *InMemory) Unacknowledged() ([]Trace, error) {
	ms.Lock()
	defer ms.Unlock()

	var msgs []Trace
	for _, v := range ms.pending {
		msgs = append(msgs, v)
	}
	return msgs, nil
}

func NewInMemory(ti TraceID) *InMemory {
	return &InMemory{
		pending: make(map[string]Trace, 1000),
		Mutex:   sync.Mutex{},
		TraceID: ti,
	}
}
