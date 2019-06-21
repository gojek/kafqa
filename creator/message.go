package creator

import "time"

//Message format for kafka message
type Message struct {
	Sequence    uint64
	ID          string
	CreatedTime time.Time
	Data        []byte
}
