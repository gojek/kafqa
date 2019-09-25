package producer

import (
	"context"
	"sync"
	"time"

	"github.com/gojekfarm/kafqa/creator"
	"github.com/gojekfarm/kafqa/tracer"
	"github.com/opentracing/opentracing-go"

	"github.com/gojekfarm/kafqa/reporter/metrics"

	"github.com/gojekfarm/kafqa/callback"
	"github.com/gojekfarm/kafqa/config"
	"github.com/gojekfarm/kafqa/logger"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type msgCreator interface {
	NewMessage() creator.Message
}

type kafkaProducer interface {
	Produce(*kafka.Message, chan kafka.Event) error
	Flush(int) int
	Events() chan kafka.Event
	Close()
	ProduceChannel() chan *kafka.Message
}

type Producer struct {
	kafkaProducer
	config   config.Producer
	messages chan creator.Message
	msgCreator
	wg        *sync.WaitGroup
	callbacks []callback.Callback
}

func (p Producer) Run(ctx context.Context) {
	go p.Poll(ctx)
	go p.runProducers(ctx)
	var i int64
	logger.Debugf("started producing to chan....")

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.messages)

		span := tracer.StartSpan("kafqa.produce.channel")
		for i = 0; p.config.TotalMessages == -1 || i < p.config.TotalMessages; i++ {
			select {
			case <-ctx.Done():
				span.Finish()
				return
			default:
				msg := p.msgCreator.NewMessage()
				p.messages <- msg
			}
		}
		span.Finish()
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
		metrics.ProducerCount()
		p.wg.Add(1)
	}
}

func (p Producer) ProduceWorker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case msg, ok := <-p.messages:
			span := opentracing.StartSpan("kafqa.produce.worker")
			if !ok {
				return
			}
			p.produceMessage(opentracing.ContextWithSpan(ctx, span), msg)
			span.Finish()
			time.Sleep(time.Millisecond * time.Duration(p.config.WorkerDelayMs))
		case <-ctx.Done():
			return
		}
	}
}

func (p Producer) produceMessage(ctx context.Context, msg creator.Message) {
	span := tracer.StartChildSpan(ctx, "kafqa.produce.kafka")
	defer span.Finish()

	msg.CreatedTime = time.Now()
	mbyte, _ := msg.Bytes()
	kafkaMsg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.config.Topic, Partition: kafka.PartitionAny},
		Value:          mbyte,
	}
	kafkaMsg.Headers = tracer.Headers(ctx, kafkaMsg.Headers)
	if err := p.kafkaProducer.Produce(&kafkaMsg, nil); err != nil {
		logger.Errorf("Error producing message to kafka: %v", err)
	} else {
		//TODO: introduce configured delay here
		for _, cb := range p.callbacks {
			cb(&kafkaMsg)
		}
	}
}

func Register(cb callback.Callback) Option {
	return func(p *Producer) {
		p.callbacks = append(p.callbacks, cb)
	}
}

type Option func(*Producer)

func New(prodCfg config.Producer, mc msgCreator, opts ...Option) (*Producer, error) {
	p, err := kafka.NewProducer(prodCfg.KafkaConfig())
	if err != nil {
		return nil, err
	}
	producer := &Producer{
		config:        prodCfg,
		kafkaProducer: p,
		messages:      make(chan creator.Message, 10000),
		wg:            &sync.WaitGroup{},
		msgCreator:    mc,
	}
	for _, opt := range opts {
		opt(producer)
	}
	return producer, nil
}

func (p Producer) Poll(ctx context.Context) {
	ticker := time.NewTicker((500 * time.Millisecond))
	for {
		select {
		case <-ticker.C:
			chanLength := len(p.kafkaProducer.ProduceChannel())
			metrics.ProducerChannelLength(chanLength)
			logger.Debugf("Producer channel length: %v", chanLength)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}
