package producer

import (
	"fmt"
	"sync"

	"github.com/gojekfarm/kafqa/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type MessageCreator interface {
	Message() []byte
}

type Producer struct {
	MessageCreator
	*kafka.Producer
	config   config.Producer
	messages chan []byte
	wg       *sync.WaitGroup
}

func (p Producer) Run() {
	go p.runProducers()
	var i uint64
	p.wg.Add(p.config.Concurrency)
	fmt.Println("started producing to chan....")

	for i = 0; i < p.config.TotalMessages; i++ {
		p.messages <- []byte(fmt.Sprintf("message-%d", i)) //p.MessageCreator.Message()
	}
	close(p.messages)
	fmt.Println("produced all messages....")
}

func (p Producer) Close() error {
	//TODO: extract out to config
	fmt.Println("closing.....")
	p.Flush(500)
	p.Producer.Close()
	p.wg.Wait()
	return nil
}

func (p Producer) runProducers() {
	for i := 0; i < p.config.Concurrency; i++ {
		fmt.Printf("running producer %d\n", i)
		go p.ProduceWorker()
	}
}

func (p Producer) ProduceWorker() {
	defer p.wg.Done()
	for msg := range p.messages {
		p.Producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.config.Topic, Partition: kafka.PartitionAny},
			Value:          msg,
		}, nil)
	}
	fmt.Println("Completed!!!")
}

func New(prodCfg config.Producer) (*Producer, error) {
	//TODO: kafka config keys could be consts
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": prodCfg.KafkaBrokers})
	if err != nil {
		return nil, err
	}
	return &Producer{
		config:   prodCfg,
		Producer: p,
		messages: make(chan []byte, 1000),
		wg:       &sync.WaitGroup{},
	}, nil
}
