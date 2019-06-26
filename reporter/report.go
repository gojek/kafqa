package reporter

import (
	"bytes"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

type Report struct {
	Messages
}

func (r *Report) String() string {
	data := [][]string{
		{"1", "Lost", strconv.FormatInt(r.Messages.Lost, 10)},
		{"2", "Sent", strconv.FormatInt(r.Messages.Sent, 10)},
		{"3", "Received", strconv.FormatInt(r.Messages.Received, 10)},
	}
	buf := bytes.NewBufferString("")
	table := tablewriter.NewWriter(buf)
	table.SetHeader([]string{"", "  Description    ", "Value"})
	for _, v := range data {
		table.Append(v)
	}
	table.Render()
	return buf.String()
}

type Messages struct {
	Lost     int64
	Sent     int64
	Received int64
}
