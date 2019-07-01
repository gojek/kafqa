package metrics

import (
	"net/http"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	messagesSent = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "messages",
		Name:      "sent",
	})
	messagesReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "messages",
		Name:      "received",
	})
)

type promClient struct {
	enabled bool
	port    int
}

var prom promClient

func AcknowledgedMessage(msg creator.Message) {
	if prom.enabled {
		messagesReceived.Inc()
	}
}

func SentMessage(msg creator.Message) {
	if prom.enabled {
		messagesSent.Inc()
	}

}

func Setup(cfg config.Prometheus) {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Error creating metrics: %v", err)
		}
	}()

	prom = promClient{enabled: cfg.Enabled, port: cfg.Port}
	if cfg.Enabled {

		prometheus.MustRegister(messagesSent)
		prometheus.MustRegister(messagesReceived)

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(cfg.BindPort(), mux)
		logger.Debugf("Enabled prometheus at /metris port: %s", cfg.BindPort())
	}
}
