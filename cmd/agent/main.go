package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gojek/kafqa/agent"
	"github.com/gojek/kafqa/agent/metrics"
	agcfg "github.com/gojek/kafqa/config/agent"
	"github.com/gojek/kafqa/logger"
)

func main() {
	cfg, err := agcfg.LoadAgentConfig()
	if err != nil {
		log.Fatalf("error loading agent config: %v", err)
	}
	logger.Setup(cfg.LogLevel())
	nav, err := agent.NewNavigator(cfg.KafkaDataDir())
	if err != nil {
		log.Fatalf("error creating navigator: %v", err)
	}

	pcli, err := metrics.Setup(cfg.Prometheus)
	if err != nil {
		log.Fatalf("error setting up metrics: %v", err)
	}
	navJob := agent.NewTopicSizeReporter(nav, pcli)

	ag := agent.New(cfg, navJob)
	go registerSignalHandler(ag)

	errs := ag.Start()
	go logErrors(errs)
	ag.Wait()
}

func logErrors(errs <-chan error) {
	for err := range errs {
		logger.Errorf("error: %v", err)
	}
}

func registerSignalHandler(ag *agent.Agent) {
	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	<-exit
	logger.Infof("[Agent.main] received interrupt")
	ag.Stop()
}
