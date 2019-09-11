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
	tags = []string{"topic", "pod_name", "deployment"}

	messagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "messages",
		Name:      "sent",
	}, tags)
	messagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "messages",
		Name:      "received",
	}, tags)
	produceLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "latency_ms",
		Name:       "produce",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, tags)
	consumeLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "latency_ms",
		Name:       "receive",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, tags)
	producerCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "producers",
		Name:      "running",
	}, tags)
	consumerCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "consumers",
		Name:      "running",
	}, tags)
	producerChannelCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "producer_channel",
		Name:      "messages_queued",
	}, tags)
)

type promClient struct {
	enabled    bool
	port       int
	pod        string
	deployment string
}

var prom promClient

func AcknowledgedMessage(msg creator.Message, topic string) {
	if prom.enabled {
		messagesReceived.WithLabelValues(topic, prom.pod, prom.deployment).Inc()
	}
}

func SentMessage(msg creator.Message, topic string) {
	if prom.enabled {
		messagesSent.WithLabelValues(topic, prom.pod, prom.deployment).Inc()
	}
}

func ConsumerLatency(dur time.Duration, topic string) {
	if prom.enabled {
		ms := dur / time.Millisecond
		consumeLatency.WithLabelValues(topic, prom.pod, prom.deployment).Observe(float64(ms))
	}
}

func ProduceLatency(dur time.Duration, topic string) {
	if prom.enabled {
		ms := dur / time.Millisecond
		produceLatency.WithLabelValues(topic, prom.pod, prom.deployment).Observe(float64(ms))
	}
}

func ProducerCount(topic string) {
	if prom.enabled {
		producerCount.WithLabelValues(topic, prom.pod, prom.deployment).Inc()
	}
}

func ConsumerCount(topic string) {
	if prom.enabled {
		consumerCount.WithLabelValues(topic, prom.pod, prom.deployment).Inc()
	}
}

func ProducerChannelLength(count int, topic string) {
	if prom.enabled {
		producerChannelCount.WithLabelValues(topic, prom.pod, prom.deployment).Add(float64(count))
	}
}

func Setup(cfg config.Prometheus) {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Error creating metrics: %v", err)
		}
	}()

	prom = promClient{enabled: cfg.Enabled, port: cfg.Port, pod: cfg.PodName, deployment: cfg.Deployment}
	if cfg.Enabled {

		prometheus.MustRegister(messagesSent)
		prometheus.MustRegister(messagesReceived)
		prometheus.MustRegister(consumeLatency)
		prometheus.MustRegister(produceLatency)
		prometheus.MustRegister(producerCount)
		prometheus.MustRegister(consumerCount)
		prometheus.MustRegister(producerChannelCount)

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
