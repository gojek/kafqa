package creator

import "fmt"

type Creator struct {
	index uint64
}

func (c *Creator) NewBytes() []byte {
	c.index++
	return []byte(fmt.Sprintf("message-%d", c.index))
}

func New() *Creator {
	return &Creator{}
}
