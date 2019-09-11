package creator

import (
	"time"

	"github.com/icrowley/fake"
	uuid "github.com/satori/go.uuid"
)

type Creator struct {
	index uint64
}

func (c *Creator) NewMessage() Message {
	c.index++
	id := uuid.NewV4()
	return Message{
		Sequence:    c.index,
		ID:          id.String(),
		CreatedTime: time.Now(),
		Data:        []byte(fake.ParagraphsN(10)),
	}
}

func New() *Creator {
	return &Creator{}
}
