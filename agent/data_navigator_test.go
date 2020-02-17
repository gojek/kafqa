package agent

import (
	"path/filepath"
	"testing"

	"github.com/gojek/kafqa/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	logger.Setup("none")
}

func TestShouldGetTopicInformation(t *testing.T) {

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

}

func TestShouldSplitTopicPartition(t *testing.T) {
	nav, _ := NewNavigator("somedir")

	topic, partition, err := nav.splitTopicPartition("something-topic-1")

	require.NoError(t, err)
	assert.Equal(t, "something-topic", topic)
	assert.Equal(t, 1, partition)
}

func TestBla(t *testing.T) {
	dir, _ := filepath.Abs("./testdata")

	di, err := getDirsSize(dir)

	require.NoError(t, err)
	assert.Equal(t, di, dirInfo{name: dir, sizeBytes: 759})
}
