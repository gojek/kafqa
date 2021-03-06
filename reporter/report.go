package reporter

import (
	"bytes"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"
)

type Report struct {
	Messages
	Time
}

func (r *Report) String() string {
	data := [][]string{
		{"1", "Messages Lost", strconv.FormatInt(r.Messages.Lost, 10)},
		{"2", "Messages Sent", strconv.FormatInt(r.Messages.Sent, 10)},
		{"3", "Messages Received", strconv.FormatInt(r.Messages.Received, 10)},
		{"3", "Min Consumption Latency Millis", strconv.FormatUint(uint64(r.Time.MinConsumption), 10)},
		{"3", "Max Consumption Latency Millis", strconv.FormatUint(uint64(r.Time.MaxConsumption), 10)},
		{"3", "App Run Time", r.Time.AppRun.String()},
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

type Time struct {
	MinConsumption uint32
	MaxConsumption uint32
	AppRun         time.Duration
}
