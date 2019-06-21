package creator

type Creator struct {
	index uint64
}

func (c *Creator) NewBytes() ([]byte, error) {
	c.index++
	return Message{Sequence: c.index}.Bytes()
}

func New() *Creator {
	return &Creator{}
}
