package config

import (
	"github.com/kelseyhightower/envconfig"
)

var application Application

func Load() error {
	err := envconfig.Process("STORE", &application.Store)
	if err != nil {
		return err
	}
	err = envconfig.Process("PRODUCER", &application.Producer)
	if err != nil {
		return err
	}
	err = envconfig.Process("CONSUMER", &application.Consumer)
	if err != nil {
		return err
	}
	var librdConfigs LibrdConfigs
	err = envconfig.Process("LIBRD", &librdConfigs)
	if err != nil {
		return err
	}
	err = envconfig.Process("APP", &application.Config)
	if err != nil {
		return err
	}
	err = envconfig.Process("PROMETHEUS", &application.Reporter.Prometheus)
	if err != nil {
		return err
	}
	err = envconfig.Process("STATSD", &application.Reporter.Statsd)
	if err != nil {
		return err
	}

	var consumerSslCfg SSL
	err = envconfig.Process("CONSUMER", &consumerSslCfg)
	if err != nil {
		return err
	}
	var producerSslCfg SSL
	err = envconfig.Process("PRODUCER", &producerSslCfg)
	if err != nil {
		return err
	}
	application.Consumer.ssl = consumerSslCfg
	application.Producer.ssl = producerSslCfg
	application.Producer.Librdconfigs = librdConfigs
	application.Consumer.LibrdConfigs = librdConfigs
	return nil
}
