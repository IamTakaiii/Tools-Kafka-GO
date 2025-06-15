package kafkago

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

func NewKafkaProducer(config *kafka.ConfigMap) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	kp := &KafkaProducer{producer: p}

	go kp.handleDeliveryReports()

	return kp, nil
}

func (kp *KafkaProducer) handleDeliveryReports() {
	for e := range kp.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
			} else {
				log.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		case kafka.Error:
			log.Printf("Producer error: %v\n", ev)
		}
	}
}

func (kp *KafkaProducer) Publish(topic, message string) error {
	err := kp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}
	return nil
}

func (kp *KafkaProducer) Close() {
	remaining := kp.producer.Flush(15000)
	if remaining > 0 {
		log.Printf("%d messages were not delivered", remaining)
	}
	kp.producer.Close()
}