package tracer

import (
	"io"
	"time"

	"github.com/gojekfarm/kafqa/logger"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jgrcfg "github.com/uber/jaeger-client-go/config"
)

func New() (opentracing.Tracer, io.Closer, error) {
	tracer, closer, err := getConfig().New(
		"kafqa",
		config.Logger(jaeger.StdLogger),
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
	}
}

func StartSpan(name string) opentracing.Span {
	return opentracing.GlobalTracer().StartSpan(name)
}
