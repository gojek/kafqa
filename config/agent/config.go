package agent

import (
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Agent
	Kafka
	DevMode bool `split_words:"true" default:"false"`
}

type Agent struct {
	ScheduleMs       int `split_words:"true" default:"100"`
	ErrorChannelSize int `split_words:"true" default:10000`
}

type Kafka struct {
	DataDir string `split_words:"true" required:"true"`
}

func LoadAgentConfig() (Config, error) {
	cfg := Config{}
	cfgs := map[string]interface{}{
		"AGENT": &cfg.Agent,
		"KAFKA": &cfg.Kafka,
	}

	err := loadConfigs(cfgs)
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (a Config) ScheduleDuration() time.Duration {
	return time.Millisecond * time.Duration(a.ScheduleMs)
}

func (a Config) KafkaDataDir() string {
	return a.Kafka.DataDir
}

func loadConfigs(configs map[string]interface{}) error {
	var errs multierror.Error
	for prefix, cfg := range configs {
		err := envconfig.Process(prefix, cfg)
		if err != nil {
			errs.Errors = append(errs.Errors, err)
		}
	}
	return errs.ErrorOrNil()
}
