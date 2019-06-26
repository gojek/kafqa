package config

import "github.com/kelseyhightower/envconfig"

var application Application

func Load() error {
	err := envconfig.Process("PRODUCER", &application.Producer)
	if err != nil {
		return err
	}
	err = envconfig.Process("CONSUMER", &application.Consumer)
	if err != nil {
		return err
	}
	err = envconfig.Process("LOG", &application.Log)
	if err != nil {
		return err
	}
	err = envconfig.Process("APP", &application.Config)
	if err != nil {
		return err
	}

	return nil
}
