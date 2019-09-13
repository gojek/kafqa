package metrics

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/gojekfarm/kafqa/config"

	"github.com/DataDog/datadog-go/statsd"
)

type StatsdReporter interface {
	Timing(name string, value time.Duration, tags []string, rate float64) error
	Incr(name string, tags []string, rate float64) error
	Count(name string, value int64, tags []string, rate float64) error
	Gauge(name string, value float64, tags []string, rate float64) error
	Close() error
}

const rate = 1.0

var metricsreporter StatsdReporter

func SetupStatsD(cfg config.Statsd) {
	var cl *statsd.Client
	if !cfg.Enabled {
		metricsreporter = new(NopReporter)
		return
	}
	var err error
	cl, err = statsd.New(Address(cfg.Host, cfg.Port))
	if err != nil {
		log.Fatalf("failed to setup statsd: %v", err)
	}
	cl.Namespace = fmt.Sprintf("%s.", strings.Trim("kafqa", "."))
	cl.Tags = cfg.Tags
	metricsreporter = cl
}

type StatsdConfig struct {
	Host    string
	Port    int
	Enabled bool
	Service string
	Tags    []string
}

func Address(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func SetReporter(reporter StatsdReporter) {
	metricsreporter = reporter
}

func Incr(metric string, tags []string) {
	if err := metricsreporter.Incr(metric, tags, rate); err != nil {
		log.Printf("Metrics Incr Error: %v", err)
	}
}

func ReportTime(startTime time.Time, tags []string, metricName string, rate float64) {
	elapsed := time.Since(startTime)
	if err := metricsreporter.Timing(metricName, elapsed, tags, rate); err != nil {
		log.Printf("Metrics Timing Error: %v - %v", err, metricName)
	}
}

func Count(name string, value int64, tags []string) {
	if err := metricsreporter.Count(name, value, tags, rate); err != nil {
		log.Printf("Metrics Count Error: %v", err)
	}
}

func Gauge(name string, value float64, tags []string) {
	if err := metricsreporter.Gauge(name, value, tags, rate); err != nil {
		log.Printf("Metrics Gauge Error: %v", err)
	}
}

func Close() error {
	return metricsreporter.Close()
}

func ReporterType() string {
	return (reflect.TypeOf(metricsreporter).String())
}
