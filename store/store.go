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

type MsgStore interface {
	Acknowledge(msg Trace) error
	Track(msg Trace) error
	Unacknowledged() ([]string, error)
	Result() Result
}

type InMemory struct {
	pending map[string]Trace
	sync.Mutex
	TraceID
	res Result
}

func (ms *InMemory) Acknowledge(msg Trace) error {
	ms.Lock()
	defer ms.Unlock()

	ms.res.Acknowledged++
	delete(ms.pending, ms.TraceID(msg))
	return nil
}

func (ms *InMemory) Track(msg Trace) error {
	ms.Lock()
	defer ms.Unlock()

	ms.res.Tracked++
	ms.pending[ms.TraceID(msg)] = msg
	return nil
}

func (ms *InMemory) Unacknowledged() ([]string, error) {
	ms.Lock()
	defer ms.Unlock()

	var msgs []string
	for _, v := range ms.pending {
		msgs = append(msgs, ms.TraceID(v))
	}
	return msgs, nil
}

type Result struct {
	Tracked      int64
	Acknowledged int64
}

func (ms *InMemory) Result() Result {
	return ms.res
}

func NewInMemory(ti TraceID) *InMemory {
	return &InMemory{
		pending: make(map[string]Trace, 1000),
		Mutex:   sync.Mutex{},
		TraceID: ti,
	}
}
