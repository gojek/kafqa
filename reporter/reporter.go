package reporter

import (
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/store"
)

type unackStore interface {
	Unacknowledged() ([]store.Trace, error)
}

type reporter struct {
	unackStore
}

var rep reporter

func Setup(u unackStore) {
	rep = reporter{
		unackStore: u,
	}
}

func GenerateReport() {
	var report Report
	unacked, _ := rep.Unacknowledged()
	report.Messages = Messages{
		Lost: len(unacked),
	}
	logger.Infof("Report:\n%s", report.String())
}
