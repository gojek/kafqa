package agent

import (
	"flag"
	"path/filepath"
	"testing"

	"github.com/gojek/kafqa/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var integration *bool

func init() {
	logger.Setup("debug")
	// Use it for running tests locally as exact directory size differs in linux
	integration = flag.Bool("integration", false, "run integration tests")
}

func TestShouldGetTopicInformation(t *testing.T) {
	if !*integration {
		logger.Infof("Skipping test: %s", t.Name())
		t.Skip()
	}

	dir, err := filepath.Abs("./testdata/datadir")
	require.NoError(t, err)
	navigator, err := NewNavigator(dir)
	require.NoError(t, err)

	tps, err := navigator.GetTopicsMetadata()

	require.NoError(t, err)
	require.Equal(t, 5, len(tps))

	assert.Equal(t, TopicPartitionInfo{topic: "__consumer-offsets", partition: 1, sizeBytes: 151}, tps[0], "consumer-offsets partition 1")
	assert.Equal(t, TopicPartitionInfo{topic: "__consumer-offsets", partition: 2, sizeBytes: 64}, tps[1], "consumer-offsets partition 2")

	assert.Equal(t, TopicPartitionInfo{topic: "some-topic", partition: 0, sizeBytes: 64}, tps[2], "sometopic p0")
	assert.Equal(t, TopicPartitionInfo{topic: "some-topic", partition: 1, sizeBytes: 64}, tps[3], "sometopic p1")
	assert.Equal(t, TopicPartitionInfo{topic: "some-topic", partition: 12, sizeBytes: 64}, tps[4], "sometopic p12")
}

func TestShouldReturnErrorForInvalidDir(t *testing.T) {
	_, err := NewNavigator("somedir")

	assert.EqualError(t, err, "stat somedir: no such file or directory")
}

func TestShouldSplitTopicPartition(t *testing.T) {

	if !*integration {
		logger.Infof("Skipping test: %s", t.Name())
		t.Skip()
	}
	dir, err := filepath.Abs("./testdata/datadir")
	require.NoError(t, err)
	nav, err := NewNavigator(dir)
	require.NoError(t, err)

	topic, partition, err := nav.splitTopicPartition("something-topic-1")

	require.NoError(t, err)
	assert.Equal(t, "something-topic", topic)
	assert.Equal(t, 1, partition)
}

func TestDirectorySizeIntegration(t *testing.T) {
	flag.Parse()
	if !*integration {
		logger.Infof("Skipping test: %s", t.Name())
		t.Skip()
	}
	dir, _ := filepath.Abs("./testdata")

	di, err := getDirsSize(dir)

	require.NoError(t, err)
	assert.Equal(t, dirInfo{name: dir, sizeBytes: 759}, di)
}
