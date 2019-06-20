package main

import (
	"fmt"
	"log"

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
}
