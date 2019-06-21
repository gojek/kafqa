package producer

import (
	"sync"

	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type msgCreator interface {
	NewBytes() ([]byte, error)
}

type Producer struct {
	*kafka.Producer
	config   config.Producer
	messages chan []byte
	msgCreator
	wg *sync.WaitGroup
}

func (p Producer) Run() {
	go p.runProducers()
	var i uint64
	p.wg.Add(p.config.Concurrency)
	logger.Debugf("started producing to chan....")

	for i = 0; i < p.config.TotalMessages; i++ {
		//TODO: report error
		mBytes, _ := p.msgCreator.NewBytes()
		p.messages <- mBytes
	}
	close(p.messages)
	logger.Debugf("produced all messages.")
}

func (p Producer) Close() error {
	logger.Debugf("closing.....")
	p.Flush(p.config.FlushTimeoutMs)
	p.Producer.Close()
	p.wg.Wait()
	return nil
}

func (p Producer) runProducers() {
	for i := 0; i < p.config.Concurrency; i++ {
		logger.Debugf("running producer %d\n", i)
		go p.ProduceWorker()
	}
}

func (p Producer) ProduceWorker() {
	defer p.wg.Done()
	for msg := range p.messages {
		kafkaMsg := kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.config.Topic, Partition: kafka.PartitionAny},
			Value:          msg,
		}
		if err := p.Producer.Produce(&kafkaMsg, nil); err != nil {
			logger.Errorf("Error producing message to kafka: %v", err)
		}
	}
}

func New(prodCfg config.Producer, mc msgCreator) (*Producer, error) {
	//TODO: kafka config keys could be consts
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": prodCfg.KafkaBrokers})
	if err != nil {
		return nil, err
	}
	return &Producer{
		config:     prodCfg,
		Producer:   p,
		messages:   make(chan []byte, 1000),
		wg:         &sync.WaitGroup{},
		msgCreator: mc,
	}, nil
}
