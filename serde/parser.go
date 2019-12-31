package serde

import (
	"github.com/gojek/kafqa/config"
	"github.com/gojek/kafqa/creator"
	"github.com/gojek/kafqa/logger"
	"github.com/jhump/protoreflect/desc/protoparse"
)

type Parser interface {
	Encoder
	Decoder
}

type Encoder interface {
	Bytes(m creator.Message) ([]byte, error)
}

type Decoder interface {
	FromBytes(bytes []byte) (creator.Message, error)
}

func New(cfg config.ProtoParser) Parser {
	defaultParser := KafqaParser{}
	if cfg.Enabled {
		parser := protoparse.Parser{}
		fileDescriptors, err := parser.ParseFiles(cfg.FilePath)
		if err != nil {
			logger.Debugf("Error on parsing proto: %v", err)
			return defaultParser
		}
		if len(fileDescriptors) == 0 {
			logger.Debugf("file descriptor is empty")
			return defaultParser
		}
		messageDescriptors := fileDescriptors[0].GetMessageTypes()
		if len(messageDescriptors) == 0 {
			logger.Debugf("message Descriptor is empty")
			return defaultParser
		}

		md := findMessageDescriptorByName(messageDescriptors, cfg.MessageName)
		if md == nil {
			logger.Debugf("message Descriptor Not found")
			return defaultParser
		}
		return ProtoParser{md: md, timestampIndex: cfg.TimestampIndex, creator: creator.New()}

	}
	return defaultParser
}
