package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/producer"
)

func main() {
	if err := config.Load(); err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	p, err := producer.New(config.App().Producer)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	fmt.Println(config.App().Producer.KafkaBrokers)
	p.Run()
	var wg sync.WaitGroup
	wg.Add(1)
	h := producer.NewHandler(p.Events(), &wg)

	go h.Handle()

	p.Close()
	wg.Wait()
}
