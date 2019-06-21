package store

import (
	"sync"

	"github.com/gojekfarm/kafqa/creator"
)

type InMemory struct {
	pending map[string]creator.Message
	sync.Mutex
}

func (ms *InMemory) Acknowledge(msg creator.Message) error {
	ms.Lock()
	defer ms.Unlock()

	delete(ms.pending, msg.ID)
	return nil
}

func (ms *InMemory) Track(msg creator.Message) error {
	ms.Lock()
	defer ms.Unlock()

	ms.pending[msg.ID] = msg
	return nil
}

func (ms *InMemory) Unacknowledged() ([]creator.Message, error) {
	ms.Lock()
	defer ms.Unlock()

	var msgs []creator.Message
	for _, v := range ms.pending {
		msgs = append(msgs, v)
	}
	return msgs, nil
}

func NewInMemory() *InMemory {
	return &InMemory{
		pending: make(map[string]creator.Message, 1000),
		Mutex:   sync.Mutex{},
	}
}
