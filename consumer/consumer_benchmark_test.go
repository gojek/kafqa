package consumer

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/gojek/kafqa/config"
	"github.com/gojek/kafqa/logger"
	"github.com/icrowley/fake"
	"github.com/stretchr/testify/mock"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func init() {
	logger.Setup("none")
}

func BenchmarkConsumer100MsgsProcessing(b *testing.B) {
	for n := 0; n <= b.N; n++ {
		config := config.Consumer{
			PollTimeoutMs:    10,
			EnableAutoCommit: true,
		}
		maxTimeout, maxMessages := time.Duration(60*time.Second), 1000
		//exit := time.After(maxTimeout)
		msg := &kafka.Message{Value: []byte(fake.ParagraphsN(1))}
		kafkaConsumer := new(consumerMock)
		stopConsumer := make(chan struct{}, 1)
		times := 0
		kafkaConsumer.On("ReadMessage", config.PollTimeout()).Run(func(ma mock.Arguments) {
			if times >= maxMessages {
				stopConsumer <- struct{}{}
			}
			times++
		}).Return(msg, nil)
		kafkaConsumer.On("Close").Return(nil)

		cb5ms := func(km *kafka.Message) { time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond) }
		cb100ms := func(km *kafka.Message) { time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond) }
		cb150ms := func(km *kafka.Message) { time.Sleep(time.Duration(rand.Intn(150)) * time.Millisecond) }
		cons := Consumer{
			consumers: []consumer{kafkaConsumer, kafkaConsumer, kafkaConsumer},
			config:    config,
			cbwg:      &sync.WaitGroup{},
			wg:        &sync.WaitGroup{},
			exit:      stopConsumer,
		}
		cons.Register(cb5ms)
		cons.Register(cb100ms)
		cons.Register(cb150ms)
		ctx, canc := context.WithTimeout(context.Background(), maxTimeout)
		defer canc()

		cons.Run(ctx)

		cons.wg.Wait()
		cons.Close()
	}
}
