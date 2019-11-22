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
	LibrdConfigs
	Jaeger
	ProtoParser
}

type Config struct {
	Environment string `default:"production"`
	DurationMs  int64  `split_words:"true" default:"10000"`
}

type LibrdConfigs struct {
	Enabled                  bool `default:"false"`
	QueueBufferingMaxMessage int  `split_words:"true" default:"100000"`
	BatchNumMessages         int  `split_words:"true" default:"10000"`
	QueuedMinMessages        int  `split_words:"true" default:"10000"`
	RequestRequiredAcks      int  `split_words:"true" default:"1"`
	StatisticsIntervalMs     int  `split_words:"true" default:"500"`
}

type Producer struct {
	Enabled          bool   `default:"true"`
	Topic            string `default:"kafqa_test" envconfig:"KAFKA_TOPIC"`
	Concurrency      int    `default:"100"`
	TotalMessages    int64  `split_words:"true" default:"10000"`
	KafkaBrokers     string `split_words:"true"`
	FlushTimeoutMs   int    `split_words:"true" default:"2000"`
	SecurityProtocol string `split_words:"true" default:"PLAINTEXT"`
	ssl              SSL
	DelayMs          int `split_words:"true" default:"1000"`
	WorkerDelayMs    int `split_words:"true" default:"50"`
	Acks             int `default:"1"`
	Librdconfigs     LibrdConfigs
	ClusterName      string `envconfig:"KAFKA_CLUSTER"`
}

type Consumer struct {
	// TODO: remove tags and load with split words while processing
	Enabled          bool   `default:"true"`
	Topic            string `default:"kafqa_test" envconfig:"KAFKA_TOPIC"`
	Concurrency      int    `default:"20"`
	KafkaBrokers     string `split_words:"true"`
	GroupID          string `split_words:"true" default:"kafqa_test_consumer"`
	OffsetReset      string `split_words:"true" default:"latest"`
	PollTimeoutMs    int64  `split_words:"true" default:"500"`
	SecurityProtocol string `split_words:"true" default:"PLAINTEXT"`
	EnableAutoCommit bool   `split_words:"true" default:"true"`
	ssl              SSL
	LibrdConfigs     LibrdConfigs
}

type SSL struct {
	CALocation          string `split_words:"true"`
	CertificateLocation string `split_words:"true"`
	KeyLocation         string `split_words:"true"`
	KeyPassword         string `split_words:"true"`
}

type Prometheus struct {
	Enabled    bool   `default:"false"`
	Port       int    `default:"9999"`
	PodName    string `envconfig:"POD_NAME"`
	Deployment string `envconfig:"DEPLOYMENT"`
}

type Statsd struct {
	Host       string
	Port       int
	Enabled    bool   `default:"false"`
	PodName    string `envconfig:"POD_NAME"`
	Deployment string `envconfig:"DEPLOYMENT"`
	Tags       []string
}

type Store struct {
	Type      string `default:"memory"`
	RunID     string `split_words:"true"`
	RedisHost string `split_words:"true"`
}

type Jaeger struct {
	Disabled         bool    `default:"true"`
	ServiceName      string  `split_words:"true" default:"kafqa"`
	ReporterLogSpans bool    `split_words:"true" default:"false"`
	SamplerType      string  `split_words:"true" default:"const"`
	SamplerParam     float64 `split_words:"true" default:"1"`
	AgentPort        int     `split_words:"true" default:"6831"`
	AgentHost        string  `split_words:"true" default:"localhost"`
}

type ProtoParser struct {
	Enabled        bool   `default:"false"`
	FilePath       string `split_words:"true"`
	MessageName    string `split_words:"true"`
	TimestampIndex int    `split_words:"true"`
}

func (p Prometheus) BindPort() string {
	return fmt.Sprintf("0.0.0.0:%d", p.Port)
}

type Reporter struct {
	Prometheus
	Statsd
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
		KafkaBootstrapServerKey:           p.KafkaBrokers,
		SecurityProtocol:                  p.SecurityProtocol,
		SSLCALocation:                     p.ssl.CALocation,
		SSLKeyLocation:                    p.ssl.KeyLocation,
		SSLKeyPassword:                    p.ssl.KeyPassword,
		SSLCertLocation:                   p.ssl.CertificateLocation,
		ProducerBatchNumMessages:          p.Librdconfigs.BatchNumMessages,
		ProducerQueueBufferingMaxMessages: p.Librdconfigs.QueueBufferingMaxMessage,
		ProduceRequestRequiredAcks:        p.Librdconfigs.RequestRequiredAcks,
		LibrdStatisticsIntervalMs:         p.Librdconfigs.StatisticsIntervalMs,
	}
}

func (c Consumer) KafkaConfig() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		KafkaBootstrapServerKey:   c.KafkaBrokers,
		ConsumerOffsetResetKey:    c.OffsetReset,
		ConsumerGroupIDKey:        c.GroupID,
		SecurityProtocol:          c.SecurityProtocol,
		SSLCALocation:             c.ssl.CALocation,
		SSLKeyLocation:            c.ssl.KeyLocation,
		SSLKeyPassword:            c.ssl.KeyPassword,
		SSLCertLocation:           c.ssl.CertificateLocation,
		EnableAutoCommit:          c.EnableAutoCommit,
		ConsumerQueuedMinMessages: c.LibrdConfigs.QueuedMinMessages,
		LibrdStatisticsIntervalMs: c.LibrdConfigs.StatisticsIntervalMs,
	}
}

func (c Consumer) PollTimeout() time.Duration {
	return time.Duration(c.PollTimeoutMs) * time.Millisecond
}

func (j Jaeger) AgentHostPort() string {
	return fmt.Sprintf("%s:%d", j.AgentHost, j.AgentPort)
}
