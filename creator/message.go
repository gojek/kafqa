package creator

import (
	"fmt"
	"time"
)

// Message format for kafka message
type Message struct {
	Sequence    uint64
	ID          string
	CreatedTime time.Time
	Data        []byte
}

func (m Message) String() string {
	return fmt.Sprintf("ID: %s sequence: %d time: %s", m.ID, m.Sequence, m.CreatedTime.Format(time.RFC3339))
}
