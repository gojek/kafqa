package reporter

import "fmt"

type LibrdKafkaStatsHandler struct {
	counters map[string][]string
	gauges   map[string][]string
	tags     []string
}

type LibrdTags struct {
	ClusterName string
	Ack         string
	Topic       string
}

func GetStats(stats, tags []string, statsJSON, level, statType string) {
	switch level {
	case "top-level":
		TopLevelStats(stats, tags, statsJSON, statType)
	case "brokers":
		BrokersStats(stats, tags, statsJSON, statType)
	}
}

func (stats LibrdKafkaStatsHandler) HandleStats(statJSON string) {
	for k, counterStat := range stats.counters {
		GetStats(counterStat, stats.tags, statJSON, k, "counter")
	}

	for k, gaugeStat := range stats.gauges {
		GetStats(gaugeStat, stats.tags, statJSON, k, "gauge")
	}
}

func NewlibrdKafkaStat(tags LibrdTags) LibrdKafkaStatsHandler {
	librdtags := []string{fmt.Sprintf("topic:%s", tags.Topic),
		fmt.Sprintf("ack:%s", tags.Ack), fmt.Sprintf("kafka_cluster:%s", tags.ClusterName)}
	return LibrdKafkaStatsHandler{defaultCounters(), defaultGauges(), librdtags}
}

func defaultCounters() map[string][]string {
	return map[string][]string{
		"top-level": {"tx", "rx", "txmsgs", "rxmsgs"},
		"brokers":   {"tx", "rx"},
	}
}

func defaultGauges() map[string][]string {
	return map[string][]string{
		"top-level": {"msg_cnt", "msg_size"},
		"brokers": {"outbuf_msg_cnt", "int_latency.p99", "int_latency.avg",
			"outbuf_latency.p99", "outbuf_latency.avg", "throttle.avg", "throttle.p99",
			"rtt.avg", "rtt.p99"},
	}
}
