package agent

import (
	"os"
	"path/filepath"

	"github.com/gojek/kafqa/logger"
)

type dirInfo struct {
	name      string
	sizeBytes int64
}

func getDirsSize(dirpath string) (dirInfo, error) {
	var size int64
	err := filepath.Walk(dirpath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		logger.Errorf("error calculating size: %s %v", dirpath, err)
		return dirInfo{}, err
	}
	return dirInfo{name: dirpath, sizeBytes: size}, nil
}
