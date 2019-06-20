package config

import "github.com/kelseyhightower/envconfig"

func Load() error {
	err := envconfig.Process("PRODUCER", &application.Producer)
	if err != nil {
		return err
	}
	err = envconfig.Process("LOG", &application.Log)
	if err != nil {
		return err
	}

	return nil
}
