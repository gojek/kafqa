package config

import "github.com/kelseyhightower/envconfig"

type Application struct {
	Producer
}

type Producer struct {
	Concurrency   int    `default:"100"`
	TotalMessages uint64 `split_words:"true" default:"10000"`
	Topic         string `default:"kafqa_test"`
	KafkaBrokers  string `split_words:"true" required:"true"`
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
