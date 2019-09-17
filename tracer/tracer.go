package tracer

import (
	"context"
	"io"
	"time"

	"github.com/gojekfarm/kafqa/logger"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jgrcfg "github.com/uber/jaeger-client-go/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func New() (opentracing.Tracer, io.Closer, error) {
	tracer, closer, err := getConfig().NewTracer(
		jgrcfg.Logger(jaeger.StdLogger),
		jgrcfg.Gen128Bit(true),
	)
	if err != nil {
		return nil, nil, err
	}
	opentracing.SetGlobalTracer(tracer)
	return tracer, closer, nil
}

func Setup() (io.Closer, error) {
	tr, closer, err := New()
	if err != nil {
		logger.Errorf("[Tracer] error setting up tracer %s", err.Error())
		return nil, err
	}
	opentracing.SetGlobalTracer(tr)
	return closer, nil
}

func getConfig() jgrcfg.Configuration {
	return jgrcfg.Configuration{
		Sampler: &jgrcfg.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &jgrcfg.ReporterConfig{
			LogSpans:            false,
			BufferFlushInterval: time.Millisecond * 500,
			//CollectorEndpoint:   "http://localhost:14268/api/traces",
		},
		ServiceName: "kafqa",
	}
}

func StartSpan(name string) opentracing.Span {
	return opentracing.GlobalTracer().StartSpan(name)
}

func StartSpanFromMessage(name string, msg *kafka.Message) opentracing.Span {
	spanCtx, err := ExtractCtx(msg)
	if err != nil {
		logger.Infof("Couldn't extract span from message %v tag: %s", err, name)
	}
	return opentracing.StartSpan(name, opentracing.ChildOf(spanCtx))
}

func StartChildSpan(ctx context.Context, name string) opentracing.Span {
	spanCtx := opentracing.SpanFromContext(ctx)
	if spanCtx == nil {
		logger.Infof("Couldnt' create child context for %s", name)
		return opentracing.StartSpan(name)
	}
	return opentracing.StartSpan(name, opentracing.ChildOf(spanCtx.Context()))
}
