package config

import (
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Application struct {
	Producer
	Consumer
	Config
	Reporter
	Store
}

type Config struct {
	Environment string `default:"production"`
	DurationMs  int64  `split_words:"true" default:"10000"`
}

type Producer struct {
	Enabled          bool   `default:"true"`
	Topic            string `default:"kafqa_test" envconfig:"KAFKA_TOPIC"`
	Concurrency      int    `default:"100"`
	TotalMessages    uint64 `split_words:"true" default:"10000"`
	KafkaBrokers     string `split_words:"true" required:"true"`
	FlushTimeoutMs   int    `split_words:"true" default:"2000"`
	SecurityProtocol string `split_words:"true" default:"PLAINTEXT"`
	ssl              SSL
}

type Consumer struct {
	// TODO: remove tags and load with split words while processing
	Enabled          bool   `default:"true"`
	Topic            string `default:"kafqa_test" envconfig:"KAFKA_TOPIC"`
	Concurrency      int    `default:"20"`
	KafkaBrokers     string `split_words:"true" required:"true"`
	GroupID          string `split_words:"true" default:"kafqa_test_consumer"`
	OffsetReset      string `split_words:"true" default:"latest"`
	PollTimeoutMs    int64  `split_words:"true" default:"500"`
	SecurityProtocol string `split_words:"true" default:"PLAINTEXT"`
	EnableAutoCommit bool   `split_words:"true" default:"true"`
	ssl              SSL
}

type SSL struct {
	CALocation          string `split_words:"true"`
	CertificateLocation string `split_words:"true"`
	KeyLocation         string `split_words:"true"`
	KeyPassword         string `split_words:"true"`
}

type Prometheus struct {
	Enabled bool `default:"false"`
	Port    int  `default:"9999"`
}

type Store struct {
	Type      string `default:"memory"`
	RunID     string `split_words:"true"`
	RedisHost string `split_words:"true"`
}

func (p Prometheus) BindPort() string {
	return fmt.Sprintf("0.0.0.0:%d", p.Port)
}

type Reporter struct {
	Prometheus
}

func App() Application {
	return application
}

func (a Application) RunDuration() time.Duration {
	return time.Duration(a.Config.DurationMs) * time.Millisecond
}

func (a Application) LogLevel() string {
	if a.DevEnvironment() {
		return "debug"
	}
	return "info"
}

func (a Application) DevEnvironment() bool {
	return a.Config.Environment == "development"
}

func (p Producer) KafkaConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		KafkaBootstrapServerKey: p.KafkaBrokers,
		SecurityProtocol:        p.SecurityProtocol,
		SSLCALocation:           p.ssl.CALocation,
		SSLKeyLocation:          p.ssl.KeyLocation,
		SSLKeyPassword:          p.ssl.KeyPassword,
		SSLCertLocation:         p.ssl.CertificateLocation,
	}
}

func (c Consumer) KafkaConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		KafkaBootstrapServerKey: c.KafkaBrokers,
		ConsumerOffsetResetKey:  c.OffsetReset,
		ConsumerGroupIDKey:      c.GroupID,
		SecurityProtocol:        c.SecurityProtocol,
		SSLCALocation:           c.ssl.CALocation,
		SSLKeyLocation:          c.ssl.KeyLocation,
		SSLKeyPassword:          c.ssl.KeyPassword,
		SSLCertLocation:         c.ssl.CertificateLocation,
		EnableAutoCommit:        c.EnableAutoCommit,
	}
}

func (c Consumer) PollTimeout() time.Duration {
	return time.Duration(c.PollTimeoutMs) * time.Millisecond
}
