package agent

import (
	"github.com/gojek/kafqa/agent/metrics"
)

type JobFn func() error
type BgJob struct {
	run func() error
	id  string
}

func (j BgJob) ID() string { return j.id }

func (j BgJob) Run() error { return j.run() }

func NewJob(id string, fn JobFn) Job {
	return BgJob{run: fn, id: id}
}

type TopicTracker struct {
	Navigator
	pcli metrics.PromClient
}

func (t TopicTracker) ID() string {
	return "Topic-Tracker"
}

func (t TopicTracker) Run() error {
	metadata, err := t.Navigator.GetTopicsMetadata()
	if err != nil {
		return err
	}
	for _, m := range metadata {
		t.pcli.ReportTopicSize(m.topic, m.partition, m.sizeBytes)
	}
	return nil
}

func NewTopicSizeReporter(nav Navigator, pcli metrics.PromClient) Job {
	return TopicTracker{Navigator: nav, pcli: pcli}
}
