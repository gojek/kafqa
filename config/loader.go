package config

import (
	"github.com/hashicorp/go-multierror"
	"github.com/kelseyhightower/envconfig"
)

var application Application

func Load() error {
	var producerSslCfg SSL
	var librdConfigs LibrdConfigs
	var consumerSslCfg SSL
	sslCfgs := map[string]interface{}{
		"CONSUMER": &consumerSslCfg,
		"PRODUCER": &producerSslCfg,
	}

	configs := map[string]interface{}{
		"STORE":        &application.Store,
		"PRODUCER":     &application.Producer,
		"CONSUMER":     &application.Consumer,
		"LIBRD":        &librdConfigs,
		"APP":          &application.Config,
		"PROMETHEUS":   &application.Reporter.Prometheus,
		"STATSD":       &application.Reporter.Statsd,
		"JAEGER":       &application.Jaeger,
		"PROTO_PARSER": &application.ProtoParser,
	}
	if err := loadConfigs(configs); err != nil {
		return err
	}
	if err := loadConfigs(sslCfgs); err != nil {
		return err
	}

	application.Consumer.ssl = consumerSslCfg
	application.Producer.ssl = producerSslCfg
	application.Producer.Librdconfigs = application.Librdconfigs
	application.Consumer.LibrdConfigs = application.Librdconfigs
	return nil
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
