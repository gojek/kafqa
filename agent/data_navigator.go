package agent

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/gojek/kafqa/logger"
)

type TopicPartitionInfo struct {
	topic     string
	partition int
	sizeBytes int64
}

type Navigator struct {
	datadir     string
	baseDataDir string
	rgx         *regexp.Regexp
}

func (n Navigator) GetTopicsMetadata() ([]TopicPartitionInfo, error) {
	files, err := ioutil.ReadDir(n.datadir)
	metadata, err := n.getPartitionInfo(files)
	logger.Debugf("navigating data dir: %s\n", n.datadir)
	if err != nil {
		logger.Debugf("Error walking dir: %s", n.datadir)
		return nil, err
	}
	return metadata, nil
}

func (n Navigator) getPartitionInfo(files []os.FileInfo) ([]TopicPartitionInfo, error) {
	var metadata []TopicPartitionInfo
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		m, err := n.getTopicPartition(f)
		if err != nil {
			return nil, fmt.Errorf("Error processing %s: %v", f.Name(), err)
		}
		metadata = append(metadata, m)
	}
	return metadata, nil
}

func (n Navigator) splitTopicPartition(name string) (string, int, error) {
	fields := n.rgx.FindAllStringSubmatch(name, -1)
	//[][]{"complete-match-1", "complete-match", "1"}
	if len(fields) < 1 {
		return "", -1, fmt.Errorf("unable to parse topic for dir: %v", name)
	}
	topic := fields[0][1]
	id := fields[0][2]
	pid, err := strconv.Atoi(id)
	if err != nil {
		return "", -1, err
	}
	return topic, pid, err
}

func (n Navigator) getTopicPartition(info os.FileInfo) (TopicPartitionInfo, error) {
	filename := info.Name()
	logger.Debugf("parsing %s\n", filename)
	topic, p, err := n.splitTopicPartition(filename)
	if err != nil {
		return TopicPartitionInfo{}, fmt.Errorf("error for topic: %s", err)
	}
	return TopicPartitionInfo{
		topic:     topic,
		partition: p,
		sizeBytes: info.Size(),
	}, nil
}

func New(datadir string) (Navigator, error) {
	rgx, err := regexp.Compile(`(?m)([\w\._\-]+?)-(\d+$)`)
	if err != nil {
		return Navigator{}, err
	}
	_, base := filepath.Split(datadir)

	//TODO: validate path existence
	return Navigator{
		datadir:     datadir,
		rgx:         rgx,
		baseDataDir: base,
	}, nil
}
