package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gojekfarm/kafqa/callback"
	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/consumer"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/producer"
	"github.com/gojekfarm/kafqa/reporter"
	"github.com/gojekfarm/kafqa/store"
)

type application struct {
	memStore *store.InMemory
	*producer.Producer
	*consumer.Consumer
	*producer.Handler
	*sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
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

	app.Producer.Run()
	app.Consumer.Run(app.ctx)
	app.WaitGroup.Add(1)

	go app.Handler.Handle()

	defer reporter.GenerateReport()
	app.Wait()
	logger.Infof("Completed.")
}

func (app *application) Close() {
	logger.Infof("closing application...")
	app.cancel()
	app.Producer.Close()
	app.Consumer.Close()
}

func setup(appCfg config.Application) (*application, error) {
	logger.Setup(appCfg.LogLevel())

	var wg sync.WaitGroup

	var err error
	kafkaProducer, err := producer.New(appCfg.Producer, creator.New())
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %v", err)
	}
	kafkaProducer.Register(callback.MessageSent)

	kafkaConsumer, err := consumer.New(appCfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}

	traceID := func(t store.Trace) string { return t.Message.ID }
	memStore := store.NewInMemory(traceID)

	kafkaConsumer.Register(callback.Acker(memStore))
	kafkaConsumer.Register(callback.LatencyTracker)
	if appCfg.DevEnvironment() {
		kafkaConsumer.Register(callback.Display)
	}

	ctx, cancel := context.WithTimeout(context.Background(), appCfg.RunDuration())

	reporter.Setup(memStore, 10, appCfg.Reporter)

	app := &application{
		memStore:  memStore,
		Producer:  kafkaProducer,
		Consumer:  kafkaConsumer,
		Handler:   producer.NewHandler(kafkaProducer.Events(), &wg, memStore),
		WaitGroup: &wg,
		ctx:       ctx,
		cancel:    cancel,
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
