package serde

import (
	"errors"
	"time"

	"github.com/gojekfarm/kafqa/logger"

	"github.com/jhump/protoreflect/desc"

	"github.com/gogo/protobuf/proto"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/jhump/protoreflect/dynamic"
)

type ProtoParser struct {
	md             *desc.MessageDescriptor
	timestampIndex int
	creator        *creator.Creator
}

func (protoParser ProtoParser) Bytes(m creator.Message) ([]byte, error) {
	return m.Data, nil
}

func (protoParser ProtoParser) FromBytes(bytes []byte) (creator.Message, error) {
	var publishTime time.Time
	creationTimestamp, err := protoParser.getTimestampFromProto(bytes)
	if err != nil {
		logger.Errorf("Cannot get the timestamp from the bytes, setting the creation time to current time", err)
		return protoParser.creator.NewMessage(bytes, time.Now()), nil
	}

	publishTime, err = ptypes.Timestamp(creationTimestamp)
	if err != nil {
		logger.Errorf("Cannot convert to time from the timestamp, setting the creation time to current time", err)
		return protoParser.creator.NewMessage(bytes, time.Now()), nil
	}
	return protoParser.creator.NewMessage(bytes, publishTime), nil
}

func findMessageDescriptorByName(messageDescriptors []*desc.MessageDescriptor, name string) *desc.MessageDescriptor {
	for _, m := range messageDescriptors {
		if m.GetFullyQualifiedName() == name {
			return m
		}
	}
	return nil
}

func (protoParser ProtoParser) getTimestampFromProto(msg []byte) (*timestamp.Timestamp, error) {
	dm := dynamic.NewMessageWithMessageFactory(protoParser.md, dynamic.NewMessageFactoryWithDefaults())
	err := proto.Unmarshal(msg, dm)
	if err != nil {
		return nil, err
	}
	fieldByNumber, err := dm.TryGetFieldByNumber(protoParser.timestampIndex)
	if err != nil {
		return nil, err
	}
	if publishTimestamp, ok := fieldByNumber.(*timestamp.Timestamp); ok {
		return publishTimestamp, nil
	}
	return nil, errors.New("incompatible data type - not able to type assert to timestamp ")
}
