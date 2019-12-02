package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	tags = []string{"topic", "pod_name", "deployment", "kafka_cluster", "ack"}

	//TODO: could add to []metrics in prom{} so we can register all
	messagesSent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafqa_messages",
		Name:      "sent",
	}, tags)
	messagesReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafqa_messages",
		Name:      "received",
	}, tags)
	produceLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "kafqa_latency_ms",
		Name:       "produce",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, tags)
	consumeLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "kafqa_latency_ms",
		Name:       "receive",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, tags)
	producerCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafqa_producers",
		Name:      "running",
	}, tags)
	consumerCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafqa_consumers",
		Name:      "running",
	}, tags)
	producerChannelCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kafqa_producer_channel",
		Name:      "messages_queued",
	}, tags)
	consumerMessageProcessingTime = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "kafqa_latency_ms",
		Name:       "consumer_message_processing",
		Objectives: map[float64]float64{0.9: 0.01, 0.99: 0.001},
	}, tags)
	consumerMessageReadTime = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "kafqa_latency_ms",
		Name:       "consumer_message_read",
		Objectives: map[float64]float64{0.9: 0.01, 0.99: 0.001},
	}, tags)
	consumerProcessingChannelLength = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kafqa_consumer_channel",
		Name:      "messages_queued",
	}, tags)
)

type promClient struct {
	enabled bool
	port    int
}

type promTags struct {
	topic        string
	podName      string
	deployment   string
	ack          string
	kafkaCluster string
}

var prom promClient
var promtags promTags

func AcknowledgedMessage(msg creator.Message, topic string) {
	if prom.enabled {
		messagesReceived.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Inc()
	}
}

func SentMessage(msg creator.Message) {
	if prom.enabled {
		messagesSent.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Inc()
	}
}

func ConsumerLatency(dur time.Duration) {
	if prom.enabled {
		ms := dur / time.Millisecond
		consumeLatency.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Observe(float64(ms))
	}
}

func ConsumerMessageProcessingTime(dur time.Duration) {
	if prom.enabled {
		ms := dur / time.Millisecond
		consumerMessageProcessingTime.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Observe(float64(ms))
	}
}

func ConsumerMessageReadTime(dur time.Duration) {
	if prom.enabled {
		ms := dur / time.Millisecond
		consumerMessageReadTime.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Observe(float64(ms))
	}
}

func ProduceLatency(dur time.Duration) {
	if prom.enabled {
		ms := dur / time.Millisecond
		produceLatency.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Observe(float64(ms))
	}
}

func ProducerCount() {
	if prom.enabled {
		producerCount.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Inc()
	}
}

func ConsumerCount() {
	if prom.enabled {
		consumerCount.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Inc()
	}
}

func ProducerChannelLength(count int) {
	if prom.enabled {
		producerChannelCount.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Add(float64(count))
	}
}

func ConsumerChannelLength(count int) {
	if prom.enabled {
		consumerProcessingChannelLength.WithLabelValues(promtags.topic, promtags.podName, promtags.deployment,
			promtags.kafkaCluster, promtags.ack).Add(float64(count))
	}
}

func Setup(cfg config.Prometheus, producerCfg config.Producer) {
	defer func() {
		if err := recover(); err != nil {
			logger.Errorf("Error creating metrics: %v", err)
		}
	}()

	promtags = promTags{topic: producerCfg.Topic, ack: strconv.Itoa(producerCfg.Acks),
		kafkaCluster: producerCfg.ClusterName, podName: cfg.PodName, deployment: cfg.Deployment}
	prom = promClient{enabled: cfg.Enabled, port: cfg.Port}
	if cfg.Enabled {

		prometheus.MustRegister(messagesSent)
		prometheus.MustRegister(messagesReceived)
		prometheus.MustRegister(consumeLatency)
		prometheus.MustRegister(produceLatency)
		prometheus.MustRegister(producerCount)
		prometheus.MustRegister(consumerCount)
		prometheus.MustRegister(producerChannelCount)
		prometheus.MustRegister(consumerMessageProcessingTime)
		prometheus.MustRegister(consumerMessageReadTime)
		prometheus.MustRegister(consumerProcessingChannelLength)

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
