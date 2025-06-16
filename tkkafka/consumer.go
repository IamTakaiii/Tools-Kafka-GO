package tkkafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

type MessageHandler interface {
	Handle(ctx context.Context, msg *kafka.Message) error
	HandleError(ctx context.Context, err error, msg *kafka.Message) (shouldCommit bool)
}

type Consumer struct {
	consumer *kafka.Consumer
	logger   zerolog.Logger
	handler  MessageHandler
	topics   []string
}

type Config struct {
	BootstrapServers string
	GroupID          string
	Topics           []string
	Username         string
	Password         string
	Mechanism        string
}

type ConfigFactory struct {
	baseConfig Config
}

func NewConfigFactory(bootstrapServers string, username string, password string) *ConfigFactory {
	if bootstrapServers == "" {
		panic("BootstrapServers cannot be empty")
	}
	if username == "" {
		panic("Kafka Username cannot be empty")
	}
	if password == "" {
		panic("Kafka Password cannot be empty")
	}
	return &ConfigFactory{
		baseConfig: Config{
			BootstrapServers: bootstrapServers,
			Username:         username,
			Password:         password,
			Mechanism:        "PLAIN",
		},
	}
}

func (f *ConfigFactory) Build(groupID string, topics []string) Config {
	newCfg := f.baseConfig
	newCfg.GroupID = groupID
	newCfg.Topics = topics
	return newCfg
}

func NewConsumer(cfg Config, handler MessageHandler) (*Consumer, error) {
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("group_id", cfg.GroupID).
		Strs("topics", cfg.Topics).
		Logger()

	if cfg.BootstrapServers == "" || cfg.GroupID == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("BootstrapServers, GroupID, and Topics are required")
	}
	if handler == nil {
		return nil, errors.New("MessageHandler cannot be nil")
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.GroupID,
		"sasl.mechanisms":    cfg.Mechanism,
		"sasl.username":      cfg.Username,
		"sasl.password":      cfg.Password,
		"security.protocol":  "SASL_SSL",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"session.timeout.ms": 6000,
	}

	logger.Info().Msg("Creating new Kafka consumer")

	c, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create Kafka consumer instance")
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	return &Consumer{
		consumer: c,
		logger:   logger,
		handler:  handler,
		topics:   cfg.Topics,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.consumer.Close()

	if err := c.consumer.SubscribeTopics(c.topics, nil); err != nil {
		c.logger.Error().Err(err).Msg("Failed to subscribe to topics")
		return
	}
	c.logger.Info().Msg("Consumer subscribed and started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msg("Shutdown signal received, closing consumer")
			return
		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.processMessage(ctx, e)
			case kafka.Error:
				if e.IsFatal() {
					c.logger.Error().Err(e).Msg("Fatal consumer error, stopping consumer")
					return
				}
				c.logger.Warn().Err(e).Msg("Non-fatal consumer error")
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) {
	processingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	msgLogger := c.logger.With().
		Str("topic", *msg.TopicPartition.Topic).
		Int32("partition", msg.TopicPartition.Partition).
		Int64("offset", int64(msg.TopicPartition.Offset)).
		Logger()

	msgLogger.Debug().Msg("Processing message")

	shouldCommit := true
	if err := c.handler.Handle(processingCtx, msg); err != nil {
		msgLogger.Error().Err(err).Msg("Message processing failed")
		shouldCommit = c.handler.HandleError(processingCtx, err, msg)
	}

	if shouldCommit {
		if _, err := c.consumer.CommitMessage(msg); err != nil {
			msgLogger.Error().Err(err).Msg("CRITICAL: Failed to commit message offset")
		} else {
			msgLogger.Debug().Msg("Message committed successfully")
		}
	} else {
		msgLogger.Warn().Msg("Skipping message commit as per handler's error logic")
	}
}

func RunConsumers(consumers ...*Consumer) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	appLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	appLogger.Info().Msgf("Starting %d consumer(s)", len(consumers))

	for _, consumer := range consumers {
		wg.Add(1)
		go consumer.Start(ctx, &wg)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	appLogger.Info().Msg("Shutdown signal received. Stopping all consumers")
	cancel()

	wg.Wait()
	appLogger.Info().Msg("All consumers stopped gracefully")
}