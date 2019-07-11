package config

import (
	"fmt"

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
	err = envconfig.Process("APP", &application.Config)
	if err != nil {
		return err
	}
	err = envconfig.Process("PROMETHEUS", &application.Reporter.Prometheus)
	if err != nil {
		return err
	}
	fmt.Println(application.Reporter)
	return nil
}
