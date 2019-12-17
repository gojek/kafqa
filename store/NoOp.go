package store

func NewNoOp() *NoOp {
	return &NoOp{
	}
}

type NoOp struct {
}

func (n *NoOp) Acknowledge(msg Trace) error {
	return nil
}

func (n *NoOp) Track(msg Trace) error {
	return nil
}

func (n *NoOp) Unacknowledged() ([]string, error) {
	return []string{}, nil
}

func (n *NoOp) Result() Result {
	return Result{}
}
