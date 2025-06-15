package tkkafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MessageHandler func(ctx context.Context, msg *kafka.Message) error

type KafkaConsumer struct {
	consumer *kafka.Consumer
	handler  MessageHandler
	topics   []string
}

func NewKafkaConsumer (consumerConfig *kafka.ConfigMap, handler MessageHandler, topics []string,) (*KafkaConsumer, error) {
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &KafkaConsumer{
		consumer: consumer,
		handler:  handler,
		topics:   topics,
	}, nil
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	err := c.consumer.SubscribeTopics(c.topics, nil)
	if err != nil {
		log.Printf("Error subscribing to topics: %v", err)
		return
	}

	log.Printf("Consumer started, listening to topics: %v", c.topics)

	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				go func() {
					if err := c.handler(ctx, e); err != nil {
						log.Printf("Error processing message: %v", err)
					}

					if _, commitErr := c.consumer.CommitMessage(e); commitErr != nil {
						log.Printf("Error committing message: %v", commitErr)
					}

					log.Printf("Processed message: %s", e.Value)
				}()
			case kafka.Error:
				log.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.IsFatal() {
					run = false
				}
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}

	log.Println("Closing consumer")
	c.consumer.Close()
}

func RunConsumers(consumers ...*KafkaConsumer) {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	for _, c := range consumers {
		wg.Add(1)
		go func(consumer *KafkaConsumer) {
			defer wg.Done()
			consumer.Start(ctx)
		}(c)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan
	log.Println("Shutdown signal received, stopping all consumers...")

	cancel()

	wg.Wait()
	log.Println("All consumers have been stopped.")
}