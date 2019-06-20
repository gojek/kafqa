package config

import (
	"github.com/kelseyhightower/envconfig"
)

type Application struct {
	Producer
}

type Producer struct {
	Concurrency    int    `default:"100"`
	Topic          string `default:"kafqa_test"`
	TotalMessages  uint64 `split_words:"true" default:"10000"`
	KafkaBrokers   string `split_words:"true" required:"true"`
	FlushTimeoutMs int    `split_words:"true" default:"2000"`
}

var application Application

func Load() error {
	err := envconfig.Process("PRODUCER", &application.Producer)
	if err != nil {
		return err
	}

	return nil
}

func App() Application {
	return application
}
