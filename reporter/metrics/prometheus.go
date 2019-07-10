package metrics

import (
	"net/http"
	"time"

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
	produceLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "latency_ms",
		Name:       "produce",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	consumeLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "latency_ms",
		Name:       "receive",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
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

func ConsumerLatency(dur time.Duration) {
	if prom.enabled {
		ms := dur / time.Millisecond
		consumeLatency.Observe(float64(ms))
	}
}

func ProduceLatency(dur time.Duration) {
	if prom.enabled {
		ms := dur / time.Millisecond
		produceLatency.Observe(float64(ms))
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
		prometheus.MustRegister(consumeLatency)
		prometheus.MustRegister(produceLatency)

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		go func() {
			err := http.ListenAndServe(cfg.BindPort(), mux)
			if err != nil {
				logger.Errorf("Error while binding to %s port, %v", cfg.BindPort(), err)
			}
		}()
		logger.Debugf("Enabled prometheus at /metris port: %s", cfg.BindPort())
	}
}
