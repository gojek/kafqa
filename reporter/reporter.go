package reporter

import (
	"fmt"
	"time"

	"github.com/gojek/kafqa/config"
	"github.com/gojek/kafqa/reporter/metrics"
	"github.com/gojek/kafqa/store"
)

type storeReporter interface {
	Result() store.Result
}

type reporter struct {
	*Latency
	srep  storeReporter
	start time.Time
}

var rep reporter

func Setup(sr storeReporter, maxNLatency int, cfg config.Reporter, producerCfg config.Producer) {
	rep = reporter{
		srep:    sr,
		Latency: NewLatencyReporter(maxNLatency),
		start:   time.Now(),
	}
	metrics.Setup(cfg.Prometheus, producerCfg)
	metrics.SetupPProf(cfg.PProf)
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
		AppRun:         time.Since(rep.start),
	}
	fmt.Printf("Report:\n%s\n", report.String())
}
