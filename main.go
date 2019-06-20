package main

import (
	"sync"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
	"github.com/gojekfarm/kafqa/producer"
)

func main() {
	logger.Init()

	if err := config.Load(); err != nil {
		logger.Fatalf("Error loading config: %v", err)
	}
	p, err := producer.New(config.App().Producer)
	if err != nil {
		logger.Fatalf("Error creating producer: %v", err)
	}
	logger.Infof("running application against %s", config.App().Producer.KafkaBrokers)
	p.Run()
	var wg sync.WaitGroup
	wg.Add(1)
	h := producer.NewHandler(p.Events(), &wg)

	go h.Handle()

	p.Close()
	wg.Wait()
	logger.Infof("Completed.")
}
