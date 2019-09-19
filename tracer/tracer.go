package tracer

import (
	"context"
	"io"
	"time"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jgrcfg "github.com/uber/jaeger-client-go/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func New(cfg config.Jaeger) (opentracing.Tracer, io.Closer, error) {
	tracer, closer, err := getConfig(cfg).NewTracer(
		jgrcfg.Logger(jaeger.StdLogger),
		jgrcfg.Gen128Bit(true),
	)
	if err != nil {
		return nil, nil, err
	}
	opentracing.SetGlobalTracer(tracer)
	return tracer, closer, nil
}

func Setup(cfg config.Jaeger) (io.Closer, error) {
	tr, closer, err := New(cfg)
	if err != nil {
		logger.Errorf("[Tracer] error setting up tracer %s", err.Error())
		return nil, err
	}
	opentracing.SetGlobalTracer(tr)
	return closer, nil
}

func getConfig(cfg config.Jaeger) jgrcfg.Configuration {
	return jgrcfg.Configuration{
		Sampler: &jgrcfg.SamplerConfig{
			Type:  cfg.SamplerType,
			Param: cfg.SamplerParam,
		},
		Reporter: &jgrcfg.ReporterConfig{
			LogSpans:            cfg.ReporterLogSpans,
			BufferFlushInterval: time.Millisecond * 500,
			//CollectorEndpoint:   "http://localhost:14268/api/traces",
		},
		Disabled:    cfg.Disabled,
		ServiceName: cfg.ServiceName,
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
