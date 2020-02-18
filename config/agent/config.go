package agent

import (
	"fmt"
	"log"
	"time"

	"github.com/gojek/kafqa/config"
	"github.com/hashicorp/go-multierror"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Agent
	Kafka
	Prometheus
	config.PProf
}

type Prometheus struct {
	Enabled    bool   `default:"false"`
	Port       int    `default:"9999"`
	Host       string `envconfig:"HOSTNAME"`
	Deployment string `envconfig:"DEPLOYMENT"`
}

type Agent struct {
	DevMode          bool `split_words:"true" default:"false"`
	ScheduleMs       int  `split_words:"true" default:"100"`
	ErrorChannelSize int  `split_words:"true" default:"10000"`
}

type Kafka struct {
	DataDir string `split_words:"true" required:"true"`
}

func LoadAgentConfig() (Config, error) {
	cfg := Config{}
	cfgs := map[string]interface{}{
		"AGENT":      &cfg.Agent,
		"KAFKA":      &cfg.Kafka,
		"PROMETHEUS": &cfg.Prometheus,
		"PPROF":      &cfg.PProf,
	}

	err := loadConfigs(cfgs)
	if err != nil {
		return cfg, err
	}
	log.Printf("loaded agent config: %+v", cfg)
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

func (a Config) LogLevel() string {
	if a.Agent.DevMode {
		return "debug"
	}
	return "info"
}

func (p Prometheus) BindPort() string {
	return fmt.Sprintf("0.0.0.0:%d", p.Port)
}
