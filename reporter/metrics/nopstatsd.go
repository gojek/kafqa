package metrics

import (
	"time"
)

type NopReporter struct{}

func (m *NopReporter) Timing(name string, value time.Duration, tags []string, rate float64) error {
	return nil
}

func (m *NopReporter) Close() error {
	return nil
}

func (m *NopReporter) Incr(name string, tags []string, rate float64) error {
	return nil
}

func (m *NopReporter) Count(name string, value int64, tags []string, rate float64) error {
	return nil
}

func (m *NopReporter) Gauge(name string, value float64, tags []string, rate float64) error {
	return nil
}
