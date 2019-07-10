package producer

import (
	"sync"
	"time"

	"github.com/gojekfarm/kafqa/callback"
	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type msgCreator interface {
	NewBytes() ([]byte, error)
}

type kafkaProducer interface {
	Produce(*kafka.Message, chan kafka.Event) error
	Flush(int) int
	Events() chan kafka.Event
	Close()
}

type Producer struct {
	kafkaProducer
	config   config.Producer
	messages chan []byte
	msgCreator
	wg        *sync.WaitGroup
	callbacks []callback.Callback
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
	logger.Infof("produced %d messages.", p.config.TotalMessages)
}

func (p *Producer) Register(cb callback.Callback) {
	p.callbacks = append(p.callbacks, cb)
}

func (p Producer) Close() error {
	logger.Infof("closing producer...")
	p.Flush(p.config.FlushTimeoutMs)
	p.kafkaProducer.Close()
	p.wg.Wait()
	logger.Infof("closed producer...")
	return nil
}

func (p Producer) runProducers() {
	for i := 0; i < p.config.Concurrency; i++ {
		logger.Debugf("running producer %d on brokers: %s for topic %s", i, p.config.KafkaBrokers, p.config.Topic)
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
		if err := p.kafkaProducer.Produce(&kafkaMsg, nil); err != nil {
			logger.Errorf("Error producing message to kafka: %v", err)
		} else {
			//TODO: introduce configured delay here
			for _, cb := range p.callbacks {
				cb(&kafkaMsg)
			}
			time.Sleep(10)
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
		config:        prodCfg,
		kafkaProducer: p,
		messages:      make(chan []byte, 1000),
		wg:            &sync.WaitGroup{},
		msgCreator:    mc,
	}, nil
}
