package serde

import (
	"testing"

	"github.com/gojek/kafqa/logger"

	"github.com/gogo/protobuf/proto"
	com_esb_userLocation "github.com/gojek/kafqa/serde/testdata"
	"github.com/golang/protobuf/ptypes"

	"github.com/gojek/kafqa/config"
	"github.com/stretchr/testify/assert"
)

func TestGetProtoFromProtoHandlesInvalidByteArray(t *testing.T) {
	validProtoFileName := "testdata/valid.proto"
	logger.Setup("")
	msg := []byte("test")
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: validProtoFileName, MessageName: "com.esb.userLocation.userLocationLogMessage", TimestampIndex: 3}

	timestampParser := New(protoParserConfig).(ProtoParser)
	timestamp, err := timestampParser.getTimestampFromProto(msg)
	message, messageErr := timestampParser.FromBytes(msg)

	assert.Nil(t, timestamp)
	assert.Error(t, err, "proto: bad wiretype")
	assert.NoError(t, messageErr)
	assert.Equal(t, msg, message.Data)

}

func TestGetTimestampFromProtoHandlesValidByteArray(t *testing.T) {
	publishTimeStamp := ptypes.TimestampNow()
	publishTime, _ := ptypes.Timestamp(publishTimeStamp)
	logger.Setup("")
	msg := &com_esb_userLocation.UserDetailsLogMessage{FirstName: "tony", LastName: "stark", EventTimestamp: publishTimeStamp}
	msgBytes, err := proto.Marshal(msg)
	validProtoFileName := "testdata/valid_multiple_message.proto"
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: validProtoFileName, MessageName: "com.esb.userLocation.userDetailsLogMessage", TimestampIndex: 3}

	timestampParser := New(protoParserConfig).(ProtoParser)
	timestamp, err := timestampParser.getTimestampFromProto(msgBytes)
	message, err := timestampParser.FromBytes(msgBytes)

	assert.NoError(t, err)
	assert.NotNil(t, timestamp)
	assert.NoError(t, err)
	assert.Equal(t, publishTimeStamp.String(), timestamp.String())
	assert.Equal(t, msgBytes, message.Data)
	assert.Equal(t, publishTime, message.CreatedTime)

}

func TestGetTimestampFromProtoHandlesValidByteArrayWithNonExistingProtoIndex(t *testing.T) {
	publishTime := ptypes.TimestampNow()
	logger.Setup("")
	msg := &com_esb_userLocation.UserDetailsLogMessage{FirstName: "tony", LastName: "stark", EventTimestamp: publishTime}
	msgBytes, err := proto.Marshal(msg)
	validProtoFileName := "testdata/valid_multiple_message.proto"
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: validProtoFileName, MessageName: "com.esb.userLocation.userDetailsLogMessage", TimestampIndex: 2}

	timestampParser := New(protoParserConfig).(ProtoParser)
	timestamp, err := timestampParser.getTimestampFromProto(msgBytes)
	message, MessageErr := timestampParser.FromBytes(msgBytes)

	assert.Nil(t, timestamp)
	assert.Error(t, err, "incompatible data type - not able to type assert to timestamp ")
	assert.NoError(t, MessageErr)
	assert.Equal(t, msgBytes, message.Data)
}
