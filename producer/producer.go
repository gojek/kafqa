package producer

import (
	"context"
	"sync"

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

func (p Producer) Run(ctx context.Context) {
	go p.runProducers(ctx)
	var i uint64
	logger.Debugf("started producing to chan....")

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.messages)

		for i = 0; i < p.config.TotalMessages; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				mBytes, _ := p.msgCreator.NewBytes()
				p.messages <- mBytes
			}
		}
		logger.Infof("produced %d messages.", p.config.TotalMessages)
	}()

}

func (p *Producer) Register(cb callback.Callback) {
	p.callbacks = append(p.callbacks, cb)
}

func (p Producer) Close() error {
	logger.Infof("closing producer...")
	p.Flush(p.config.FlushTimeoutMs)
	p.wg.Wait()
	p.kafkaProducer.Close()
	logger.Infof("closed producer...")
	return nil
}

func (p Producer) runProducers(ctx context.Context) {
	for i := 0; i < p.config.Concurrency; i++ {
		logger.Debugf("running producer %d on brokers: %s for topic %s", i, p.config.KafkaBrokers, p.config.Topic)
		go p.ProduceWorker(ctx)
		p.wg.Add(1)
	}
}

func (p Producer) ProduceWorker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case msg, ok := <-p.messages:
			if !ok {
				return
			}
			p.produceMessage(msg)
		case <-ctx.Done():
			return
		}
	}
}

func (p Producer) produceMessage(msg []byte) {
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
	}
}

func New(prodCfg config.Producer, mc msgCreator) (*Producer, error) {
	p, err := kafka.NewProducer(prodCfg.KafkaConfig())
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
