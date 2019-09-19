package reporter

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gojekfarm/kafqa/logger"

	"github.com/gojekfarm/kafqa/reporter/metrics"
	"github.com/savaki/jq"
)

func GetValueFromJq(parseStr, statJSON string) ([]byte, error) {
	op, _ := jq.Parse(parseStr)
	data := []byte(statJSON)
	value, err := op.Apply(data)
	return value, err
}

func BrokersStats(stats, tags []string, statJSON, statsType string) {
	value, _ := GetValueFromJq(".brokers", statJSON)
	var brokerStat map[string]interface{}
	err := json.Unmarshal([]byte(string(value)), &brokerStat)
	if err != nil {
		logger.Errorf("Error while unmarshalling, err:%v", err)
		return
	}

	for brokerName := range brokerStat {
		tags = append(tags, fmt.Sprintf("broker:%s", brokerName))
		for _, stat := range stats {
			metricName := fmt.Sprintf("librd.brokers.%s", stat)
			value, err := GetValueFromJq(fmt.Sprintf(".brokers.%s.%s", brokerName, stat), statJSON)
			if err != nil {
				logger.Errorf("Error while parsing the librd stats, err:%v", err)
				value = []byte("0.0")
			}
			switch statsType {
			case "counter":
				valueInt, _ := strconv.ParseInt(string(value), 10, 64)
				metrics.Count(metricName, valueInt, tags)

			case "gauge":
				valueFloat, _ := strconv.ParseFloat(string(value), 64)
				metrics.Gauge(metricName, valueFloat, tags)
			}
		}
	}
}

func TopLevelStats(stats, tags []string, statJSON, statsType string) {
	for _, stat := range stats {
		metricName := fmt.Sprintf("librd.%s", stat)
		value, err := GetValueFromJq(fmt.Sprintf(".%s", stat), statJSON)
		if err != nil {
			logger.Errorf("Error while parsing the librd stats, err:%v", err)
			value = []byte("0.0")
		}
		switch statsType {
		case "counter":
			valueInt, _ := strconv.ParseInt(string(value), 10, 64)
			metrics.Count(metricName, valueInt, tags)

		case "gauge":
			valueFloat, _ := strconv.ParseFloat(string(value), 64)
			metrics.Gauge(metricName, valueFloat, tags)
		}
	}
}
