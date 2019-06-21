package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

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

	logger.Infof("Completed.")

	//TODO: extract as config
	time.Sleep(30 * time.Second)
	app.Close()
	defer reporter.GenerateReport()
}

func (app application) Close() {
	logger.Infof("closing application...")
	app.cancel()
	app.Producer.Close()
	app.Consumer.Close()
	app.Wait()
}

func setup(appCfg config.Application) (*application, error) {
	logger.Setup(appCfg.Log.Level)

	var wg sync.WaitGroup

	var err error
	kafkaProducer, err := producer.New(appCfg.Producer, creator.New())
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %v", err)
	}
	kafkaConsumer, err := consumer.New(appCfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}

	traceID := func(t store.Trace) string { return t.Message.ID }
	memStore := store.NewInMemory(traceID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)

	reporter.Setup(memStore)

	return &application{
		memStore:  memStore,
		Producer:  kafkaProducer,
		Consumer:  kafkaConsumer,
		Handler:   producer.NewHandler(kafkaProducer.Events(), &wg, memStore),
		WaitGroup: &wg,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}
