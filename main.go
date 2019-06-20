package main

import (
	"log"
	"sync"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/producer"
)

func main() {
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	appCfg := config.App()
	logger.Init(appCfg.Log.Level)

	p, err := producer.New(appCfg.Producer, creator.New())
	if err != nil {
		logger.Fatalf("Error creating producer: %v", err)
	}
	logger.Infof("running application against %s", appCfg.Producer.KafkaBrokers)
	p.Run()
	var wg sync.WaitGroup
	wg.Add(1)
	h := producer.NewHandler(p.Events(), &wg)

	go h.Handle()

	p.Close()
	wg.Wait()
	logger.Infof("Completed.")
}
