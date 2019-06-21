package reporter

import "fmt"

type Report struct {
	Messages
}

func (r *Report) String() string {
	return fmt.Sprintf(`
    Messages
        Lost: %d
    `, r.Messages.Lost)
}

type Messages struct {
	Lost int
}
