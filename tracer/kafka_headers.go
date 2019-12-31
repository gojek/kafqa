package tracer

import (
	"context"

	"github.com/gojek/kafqa/logger"
	"github.com/opentracing/opentracing-go"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type KafkaHeaders []kafka.Header

func (h *KafkaHeaders) Set(key, val string) {
	for i, rec := range *h {
		if rec.Key == key {
			(*h)[i].Value = []byte(val)
			return
		}
	}
	*h = append(*h, kafka.Header{Key: key, Value: []byte(val)})
}

func (h *KafkaHeaders) ForeachKey(handler func(key, val string) error) error {
	for _, kh := range *h {
		if err := handler(kh.Key, string(kh.Value)); err != nil {
			return err
		}
	}
	return nil
}

func Headers(ctx context.Context, msgHeaders []kafka.Header) []kafka.Header {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return msgHeaders
	}
	headers := KafkaHeaders(msgHeaders)
	err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, &headers)
	if err != nil {
		logger.Errorf("Couldnt' inject headers to tracer")
	}
	return headers
}

func ExtractCtx(message *kafka.Message) (opentracing.SpanContext, error) {
	headers := KafkaHeaders(message.Headers)
	return opentracing.GlobalTracer().Extract(opentracing.TextMap, &headers)
}
