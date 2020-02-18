package metrics

import (
	"net/http"
	"strconv"

	agcfg "github.com/gojek/kafqa/config/agent"
	"github.com/gojek/kafqa/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	tags                     = []string{"deployment", "kafka_cluster", "host"}
	topicPartitionSizeInDisk = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "agent",
		Name:      "topic_partition_disk_size_bytes",
	}, append(tags, "topic", "partition"))
)

type PromClient struct {
	enabled bool
	port    int
	Tags
}

type Tags struct {
	deployment   string
	kafkaCluster string
	broker       string
}

func Setup(cfg agcfg.Prometheus, collectors ...prometheus.Collector) (PromClient, error) {
	if cfg.Enabled {
		err := prometheus.Register(topicPartitionSizeInDisk)
		if err != nil {
			return PromClient{}, err
		}

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		go func() {
			err := http.ListenAndServe(cfg.BindPort(), mux)
			if err != nil {
				logger.Errorf("Error while binding to %s port, %v", cfg.BindPort(), err)
			}
		}()
		logger.Debugf("Enabled prometheus at /metris port: %s", cfg.BindPort())
		tags := Tags{broker: cfg.Host, deployment: cfg.Deployment}
		return PromClient{port: cfg.Port, Tags: tags, enabled: cfg.Enabled}, nil
	}
	logger.Debugf("Prometheus metrics disabled")
	return PromClient{}, nil
}

func (pc PromClient) ReportTopicSize(topic string, partition int, sizeBytes int64) {
	if pc.enabled {
		g, err := topicPartitionSizeInDisk.GetMetricWith(
			prometheus.Labels{
				"deployment":    pc.Tags.deployment,
				"kafka_cluster": pc.Tags.kafkaCluster,
				"host":          pc.Tags.broker,
				"topic":         topic,
				"partition":     strconv.Itoa(partition),
			},
		)
		if err != nil {
			logger.Errorf("error building prometheus metrics: %v", err)
			return
		}
		g.Set(float64(sizeBytes))
	}
}
