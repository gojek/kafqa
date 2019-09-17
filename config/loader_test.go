package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldLoadConfig(t *testing.T) {
	envs := map[string]string{
		"PRODUCER_KAFKA_BROKERS":  "ip:port",
		"PRODUCER_TOTAL_MESSAGES": "100000",
		"KAFKA_TOPIC":             "producer_topic",
		"CONSUMER_GROUP_ID":       "consumer_group_id",
	}

	older := setEnvs(envs)
	defer setEnvs(older)

	err := Load()

	require.NoError(t, err)
	require.NotNil(t, application.Producer)
	assert.Equal(t, "producer_topic", application.Producer.Topic)
	assert.Equal(t, "ip:port", application.Producer.KafkaBrokers)
	assert.Equal(t, "consumer_group_id", application.Consumer.GroupID)
}

func TestShouldLoadKafkaSSLConfig(t *testing.T) {
	envs := map[string]string{
		"PRODUCER_TOTAL_MESSAGES":       "0",
		"CONSUMER_CA_LOCATION":          "ca.crt",
		"CONSUMER_CERTIFICATE_LOCATION": "cert.crt",
		"CONSUMER_KEY_LOCATION":         "cons.key",
	}
	older := setEnvs(envs)
	defer setEnvs(older)

	err := Load()

	require.NoError(t, err)
	kafkaCfg := *application.Consumer.KafkaConfig()
	assert.Equal(t, "ca.crt", kafkaCfg[SSLCALocation])
	assert.Equal(t, "cert.crt", kafkaCfg[SSLCertLocation])
	assert.Equal(t, "cons.key", kafkaCfg[SSLKeyLocation])
}

func setEnvs(envs map[string]string) map[string]string {
	backup := make(map[string]string)
	for k, v := range envs {
		backup[k] = os.Getenv(k)
		os.Setenv(k, v)
	}
	return backup
}
