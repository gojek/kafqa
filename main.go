package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gojekfarm/kafqa/reporter/metrics"

	"github.com/gojekfarm/kafqa/callback"
	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/consumer"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/producer"
	"github.com/gojekfarm/kafqa/reporter"
	"github.com/gojekfarm/kafqa/store"
	"github.com/gojekfarm/kafqa/tracer"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type application struct {
	msgStore store.MsgStore
	*producer.Producer
	*consumer.Consumer
	*producer.Handler
	*sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	consumerWg *sync.WaitGroup
}

func main() {
	if err := config.Load(); err != nil {
		log.Fatalf("error loading config: %v", err)
	}
	appCfg := config.App()
	app, err := setup(appCfg)
	if err != nil {
		log.Fatalf("error initializing app: %v", err)
	}

	logger.Infof("running application against %s", appCfg.Producer.KafkaBrokers)

	if app.Consumer != nil {
		app.Consumer.Run(app.ctx)
	}

	// Introducing delay since consumers can consume quickly
	time.Sleep(time.Millisecond * time.Duration(appCfg.Producer.DelayMs))

	if app.Producer != nil {
		app.Producer.Run(app.ctx)
		app.WaitGroup.Add(1)
		go app.Handler.Handle()
	}

	defer reporter.GenerateReport()
	app.Wait()
	logger.Infof("Completed.")
}

func (app *application) Close() {
	logger.Infof("closing application...")
	app.cancel()
	if app.Producer != nil {
		app.Producer.Close()
	}
	if app.Consumer != nil {
		app.Consumer.Close()
	}
}

func (app *application) Wait() {
	app.WaitGroup.Wait()
	app.consumerWg.Wait()
}

func getProducer(cfg config.Producer) (*producer.Producer, error) {
	if !cfg.Enabled {
		logger.Infof("Producer is not enabled")
		return nil, nil
	}
	var err error
	kafkaProducer, err := producer.New(cfg, creator.New(),
		producer.Register(callback.MessageSent),
		producer.Register(func(msg *kafka.Message) { time.Sleep(200) }),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %v", err)
	}
	return kafkaProducer, nil
}

func getConsumer(appCfg config.Application, ms store.MsgStore, wg *sync.WaitGroup) (*consumer.Consumer, error) {
	if !appCfg.Consumer.Enabled {
		logger.Infof("Consumer is not enabled")
		return nil, nil
	}
	kafkaConsumer, err := consumer.New(appCfg.Consumer,
		consumer.Register(callback.Acker(ms)),
		consumer.Register(callback.LatencyTracker),
		consumer.WaitGroup(wg))
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}
	if appCfg.DevEnvironment() {
		kafkaConsumer.Register(callback.Display)
	}
	return kafkaConsumer, nil
}

func setup(appCfg config.Application) (*application, error) {
	logger.Setup(appCfg.LogLevel())
	metrics.SetupStatsD(appCfg.Reporter.Statsd)
	tracer.Setup()

	var wg sync.WaitGroup

	kafkaProducer, err := getProducer(appCfg.Producer)
	if err != nil {
		return nil, err
	}

	traceID := func(t store.Trace) string { return t.Message.ID }
	ms, err := store.New(appCfg.Store, traceID)
	if err != nil {
		return nil, err
	}
	var consWg sync.WaitGroup
	kafkaConsumer, err := getConsumer(appCfg, ms, &consWg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), appCfg.RunDuration())

	reporter.Setup(ms, 10, appCfg.Reporter)

	app := &application{
		msgStore:   ms,
		Producer:   kafkaProducer,
		Consumer:   kafkaConsumer,
		consumerWg: &consWg,
		WaitGroup:  &wg,
		ctx:        ctx,
		cancel:     cancel,
	}
	if kafkaProducer != nil {
		app.Handler = producer.NewHandler(kafkaProducer.Events(), &wg, ms, appCfg.Producer.Topic)
	}
	go app.registerSignalHandler()
	return app, nil
}

func (app *application) registerSignalHandler() {
	defer app.WaitGroup.Done()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	app.WaitGroup.Add(1)
	for {
		select {
		case <-app.ctx.Done():
			logger.Debugf("context done, closing application")
			app.Close()
			return
		case <-exit:
			logger.Debugf("Received interrupt, closing application")
			app.Close()
			return
		}
	}
}
