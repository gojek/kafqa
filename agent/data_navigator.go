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

const expr = `(?m)([\w\._\-]+?)-(\d+$)`

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
	if err != nil {
		return nil, err
	}
	metadata, err := n.getPartitionInfo(files)
	if err != nil {
		logger.Debugf("error getting information for dir: %s", n.datadir)
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
		di, err := getDirsSize(filepath.Join(n.datadir, f.Name()))
		if err != nil {
			return nil, fmt.Errorf("error getting size %s: %v", f.Name(), err)
		}
		m, err := n.getTopicPartition(di)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %v", f.Name(), err)
		}
		metadata = append(metadata, m)
	}
	return metadata, nil
}

func (n Navigator) splitTopicPartition(name string) (topic string, partition int, err error) {
	fields := n.rgx.FindAllStringSubmatch(name, -1)
	// [][]{"complete-match-1", "complete-match", "1"}
	if len(fields) < 1 {
		return "", -1, fmt.Errorf("unable to parse topic for dir: %v", name)
	}
	topic = fields[0][1]
	id := fields[0][2]
	partition, err = strconv.Atoi(id)
	if err != nil {
		return "", -1, err
	}
	return
}

func (n Navigator) getTopicPartition(dir dirInfo) (TopicPartitionInfo, error) {
	topic, p, err := n.splitTopicPartition(dir.name)
	if err != nil {
		return TopicPartitionInfo{}, fmt.Errorf("error for topic: %s", err)
	}
	return TopicPartitionInfo{
		topic:     topic,
		partition: p,
		sizeBytes: dir.sizeBytes,
	}, nil
}

func NewNavigator(datadir string) (Navigator, error) {
	rgx, err := regexp.Compile(expr)
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
