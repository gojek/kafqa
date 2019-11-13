package serde

import (
	"reflect"
	"testing"

	"github.com/gojekfarm/kafqa/logger"

	"github.com/gojekfarm/kafqa/config"
	"github.com/stretchr/testify/assert"
)

func TestNewReturnsKafqaParserWhenConfigIsDisabled(t *testing.T) {
	validProtoFileName := "testdata/valid.proto"
	protoParserConfig := config.ProtoParser{Enabled: false, FilePath: validProtoFileName, TimestampIndex: 1}

	timestampParser := New(protoParserConfig)

	assert.Equal(t, "serde.KafqaParser", reflect.TypeOf(timestampParser).String())
	assert.NotNil(t, timestampParser)
}

func TestNewReturnsProtoParserForValidProto(t *testing.T) {
	validProtoFileName := "testdata/valid.proto"
	logger.Setup("")
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: validProtoFileName, MessageName: "com.esb.userLocation.userLocationLogMessage", TimestampIndex: 1}

	timestampParser := New(protoParserConfig).(ProtoParser)

	assert.NotNil(t, timestampParser)
	assert.Equal(t, len(timestampParser.md.GetFields()), 1)
	assert.Equal(t, timestampParser.md.GetFullyQualifiedName(), "com.esb.userLocation.userLocationLogMessage")

}

func TestReturnsKafqaParserForValidProtoAndInvalidName(t *testing.T) {
	validProtoFileName := "testdata/valid.proto"
	logger.Setup("")
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: validProtoFileName, MessageName: "com.esb.userLocation.testMessage", TimestampIndex: 1}

	timestampParser := New(protoParserConfig).(KafqaParser)

	assert.NotNil(t, timestampParser)
}

func TestReturnsKafqaParserNewProtoParserForInvalidValidProtoPath(t *testing.T) {
	invalidProtoFileName := "testdata/xyz.proto"
	logger.Setup("")
	protoParserConfig := config.ProtoParser{Enabled: false, FilePath: invalidProtoFileName, TimestampIndex: 1}

	timestampParser := New(protoParserConfig).(KafqaParser)

	assert.NotNil(t, timestampParser)
}

func TestReturnsKafqaParserNewProtoParserForInvalidValidProtoFile(t *testing.T) {
	invalidProtoFileName := "testdata/invalid.proto"
	logger.Setup("")
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: invalidProtoFileName, TimestampIndex: 1}

	timestampParser := New(protoParserConfig).(KafqaParser)

	assert.NotNil(t, timestampParser)
}

func TestReturnsNewProtoParserToReadOnlyFirstMessageForValidProtoWithMultipleMessage(t *testing.T) {
	validProtoFileName := "testdata/valid_multiple_message.proto"
	logger.Setup("")
	protoParserConfig := config.ProtoParser{Enabled: true, FilePath: validProtoFileName, MessageName: "com.esb.userLocation.userDetailsLogMessage", TimestampIndex: 1}

	timestampParser := New(protoParserConfig).(ProtoParser)

	assert.NotNil(t, timestampParser)
	assert.Equal(t, len(timestampParser.md.GetFields()), 3)
	assert.Equal(t, timestampParser.md.GetFullyQualifiedName(), "com.esb.userLocation.userDetailsLogMessage")
}
