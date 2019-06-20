package config

type Application struct {
	Producer
	Log
}

type Log struct {
	Level string `split_words:"true" default:"info"`
}

type Producer struct {
	Concurrency    int    `default:"100"`
	Topic          string `default:"kafqa_test"`
	TotalMessages  uint64 `split_words:"true" default:"10000"`
	KafkaBrokers   string `split_words:"true" required:"true"`
	FlushTimeoutMs int    `split_words:"true" default:"2000"`
}

var application Application

func App() Application {
	return application
}
