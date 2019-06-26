package reporter

import (
	"fmt"

	"github.com/gojekfarm/kafqa/store"
)

type storeReporter interface {
	Result() store.Result
}

type reporter struct {
	srep storeReporter
}

var rep reporter

func Setup(sr storeReporter) {
	rep = reporter{
		srep: sr,
	}
}

func GenerateReport() {
	var report Report
	sres := rep.srep.Result()
	report.Messages = Messages{
		Sent:     sres.Tracked,
		Received: sres.Acknowledged,
		Lost:     sres.Tracked - sres.Acknowledged,
	}
	fmt.Printf("Report:\n%s\n", report.String())
}
