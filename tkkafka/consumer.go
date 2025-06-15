package tkkafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MessageHandler func(ctx context.Context, msg *kafka.Message) error

type ErrorHandler func(err error, msg *kafka.Message) (shouldCommit bool)

type SASLConfig struct {
	Username  string
	Password  string
	Mechanism string
}

type Config struct {
	BootstrapServers  string
	GroupID           string
	Topics            []string
	SASL              *SASLConfig
	AutoOffsetReset   string
	EnableAutoCommit  bool
	PollTimeout       time.Duration
	ProcessingTimeout time.Duration
}

type BaseConfig struct {
	BootstrapServers  string
	SASL              *SASLConfig
	AutoOffsetReset   string
	EnableAutoCommit  bool
	PollTimeout       time.Duration
	ProcessingTimeout time.Duration
}

type ConfigFactory struct {
	baseConfig BaseConfig
}

func NewConfigFactory(base BaseConfig) *ConfigFactory {
	return &ConfigFactory{baseConfig: base}
}

func (f *ConfigFactory) Build(groupID string, topics []string) Config {
	return Config{
		BootstrapServers:  f.baseConfig.BootstrapServers,
		SASL:              f.baseConfig.SASL,
		AutoOffsetReset:   f.baseConfig.AutoOffsetReset,
		EnableAutoCommit:  f.baseConfig.EnableAutoCommit,
		PollTimeout:       f.baseConfig.PollTimeout,
		ProcessingTimeout: f.baseConfig.ProcessingTimeout,
		GroupID:           groupID,
		Topics:            topics,
	}
}

type Consumer struct {
	consumer          *kafka.Consumer
	handler           MessageHandler
	errorHandler      ErrorHandler
	topics            []string
	logger            *slog.Logger
	pollTimeout       time.Duration
	processingTimeout time.Duration
}

func defaultErrorHandler(logger *slog.Logger) ErrorHandler {
	return func(err error, msg *kafka.Message) bool {
		logger.Error("Message processing failed",
			slog.String("topic", *msg.TopicPartition.Topic),
			slog.String("error", err.Error()),
		)
		return false
	}
}

func NewConsumer(cfg Config, handler MessageHandler, errorHandler ErrorHandler, logger *slog.Logger) (*Consumer, error) {
	cfg.setDefaults()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer configuration: %w", err)
	}

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	if errorHandler == nil {
		errorHandler = defaultErrorHandler(logger)
	}

	kafkaConfig, err := cfg.toKafkaConfigMap()
	if err != nil {
		return nil, fmt.Errorf("failed to build kafka config: %w", err)
	}

	c, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &Consumer{
		consumer:          c,
		handler:           handler,
		errorHandler:      errorHandler,
		topics:            cfg.Topics,
		logger:            logger.With(slog.String("group_id", cfg.GroupID)),
		pollTimeout:       cfg.PollTimeout,
		processingTimeout: cfg.ProcessingTimeout,
	}, nil
}

func (c *Config) setDefaults() {
	if c.PollTimeout == 0 {
		c.PollTimeout = 100 * time.Millisecond
	}
	if c.ProcessingTimeout == 0 {
		c.ProcessingTimeout = 30 * time.Second
	}
}

func (c *Config) validate() error {
	if c.BootstrapServers == "" {
		return errors.New("BootstrapServers cannot be empty")
	}
	if c.GroupID == "" {
		return errors.New("GroupID cannot be empty")
	}
	if len(c.Topics) == 0 {
		return errors.New("topics list cannot be empty")
	}
	return nil
}

func (c *Config) toKafkaConfigMap() (*kafka.ConfigMap, error) {
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":               c.BootstrapServers,
		"group.id":                        c.GroupID,
		"auto.offset.reset":               c.AutoOffsetReset,
		"enable.auto.commit":              c.EnableAutoCommit,
		"go.events.channel.enable":        true,
		"enable.partition.eof":            false,
		"go.application.rebalance.enable": true,
	}

	if c.SASL != nil {
		if err := kafkaConfig.SetKey("security.protocol", "SASL_SSL"); err != nil {
			return nil, err
		}
		if err := kafkaConfig.SetKey("sasl.mechanisms", c.SASL.Mechanism); err != nil {
			return nil, err
		}
		if err := kafkaConfig.SetKey("sasl.username", c.SASL.Username); err != nil {
			return nil, err
		}
		if err := kafkaConfig.SetKey("sasl.password", c.SASL.Password); err != nil {
			return nil, err
		}
	}
	return kafkaConfig, nil
}

func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("Starting consumer", slog.Any("topics", c.topics))

	if err := c.consumer.SubscribeTopics(c.topics, nil); err != nil {
		c.logger.Error("Failed to subscribe to topics", slog.String("error", err.Error()))
		return
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context cancelled. Closing Kafka consumer.")
			if err := c.consumer.Close(); err != nil {
				c.logger.Error("Error closing consumer", slog.String("error", err.Error()))
			}
			return
		default:
			pollTimeoutMs := int(c.pollTimeout.Milliseconds())
			ev := c.consumer.Poll(pollTimeoutMs)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.processMessage(ctx, e)
			case kafka.Error:
				c.logger.Error("Consumer error", slog.String("code", e.Code().String()), slog.String("error", e.Error()))
				if e.IsFatal() {
					c.logger.Error("Fatal consumer error encountered. Shutting down.")
					return
				}
			default:
				c.logger.Debug("Ignored event", slog.String("event_type", fmt.Sprintf("%T", e)))
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) {
	processingCtx, cancel := context.WithTimeout(ctx, c.processingTimeout)
	defer cancel()

	shouldCommit := true
	if err := c.handler(processingCtx, msg); err != nil {
		shouldCommit = c.errorHandler(err, msg)
	}

	if shouldCommit {
		if _, err := c.consumer.CommitMessage(msg); err != nil {
			c.logger.Error("CRITICAL: Failed to commit message offset",
				slog.String("topic", *msg.TopicPartition.Topic),
				slog.String("error", err.Error()),
			)
		} else {
			c.logger.Debug("Message committed successfully")
		}
	}
}

func RunConsumers(consumers ...*Consumer) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	slog.Info(fmt.Sprintf("Starting %d consumer(s)...", len(consumers)))

	for _, consumer := range consumers {
		wg.Add(1)
		go func(c *Consumer) {
			defer wg.Done()
			c.Start(ctx)
		}(consumer)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	slog.Info("Shutdown signal received. Stopping all consumers...")
	cancel()

	wg.Wait()
	slog.Info("All consumers have stopped gracefully.")
}
