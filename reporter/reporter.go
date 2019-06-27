package reporter

import (
	"fmt"
	"time"

	"github.com/gojekfarm/kafqa/store"
)

type storeReporter interface {
	Result() store.Result
}

type reporter struct {
	*Latency
	srep storeReporter
}

var rep reporter

func Setup(sr storeReporter, maxNLatency int) {
	rep = reporter{
		srep:    sr,
		Latency: NewLatencyReporter(maxNLatency),
	}
}

func ConsumptionDelay(t time.Duration) {
	tms := t / time.Millisecond
	rep.Latency.Push(uint32(tms))
}

func GenerateReport() {
	var report Report
	sres := rep.srep.Result()
	report.Messages = Messages{
		Sent:     sres.Tracked,
		Received: sres.Acknowledged,
		Lost:     sres.Tracked - sres.Acknowledged,
	}
	report.Time = Time{
		MinConsumption: rep.Latency.Min(),
		MaxConsumption: rep.Latency.Max(),
	}
	fmt.Printf("Report:\n%s\n", report.String())
}
