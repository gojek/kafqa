package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/producer"
	"github.com/gojekfarm/kafqa/reporter"
	"github.com/gojekfarm/kafqa/store"
)

type application struct {
	memStore *store.InMemory
	*producer.Producer
	*producer.Handler
	*sync.WaitGroup
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
	app.WaitGroup.Add(1)

	go app.Handler.Handle()

	logger.Infof("Completed.")

	app.Close()
	defer reporter.GenerateReport()
}

func (app application) Close() {
	app.Producer.Close()
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
	traceID := func(t store.Trace) string { return t.Message.ID }
	memStore := store.NewInMemory(traceID)

	reporter.Setup(memStore)

	return &application{
		memStore:  memStore,
		Producer:  kafkaProducer,
		Handler:   producer.NewHandler(kafkaProducer.Events(), &wg, memStore),
		WaitGroup: &wg,
	}, nil
}
