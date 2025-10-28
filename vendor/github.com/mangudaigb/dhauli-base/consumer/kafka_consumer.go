package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
	messages "github.com/mangudaigb/dhauli-base/types/message"
	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, log *logger.Logger, msg kafka.Message) (*messages.MessageResponse, error)

type KafkaConsumer struct {
	log     *logger.Logger
	handler Handler
	reader  *kafka.Reader
	writer  *kafka.Writer
}

func NewKafkaConsumer(cfg *config.Config, logger *logger.Logger, handler Handler) *KafkaConsumer {
	readerConfig := kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		GroupID:  cfg.Kafka.GroupId,
		Topic:    cfg.Kafka.Topic,
		MaxBytes: cfg.Kafka.MaxBytes,
	}

	kReader := kafka.NewReader(readerConfig)

	kWriter := kafka.Writer{
		Addr:  kafka.TCP(cfg.Kafka.Brokers...),
		Topic: cfg.Kafka.RouterTopic,
	}

	return &KafkaConsumer{
		log:     logger,
		handler: handler,
		reader:  kReader,
		writer:  &kWriter,
	}
}

func (c *KafkaConsumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			c.log.Errorf("Context cancelled. Closing kafka reader...")
			return c.reader.Close()

		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if err.Error() == "context canceled" {
					c.log.Errorf("Fetch consumer cancelled due to context cancellation: %v", err)
					continue
				}
				c.log.Errorf("Error while fetching consumer: %v", err)
				time.Sleep(3 * time.Second)
				continue
			}

			response, handleErr := c.handler(ctx, c.log, msg)
			if handleErr != nil {
				c.log.Errorf("Error while handling consumer: %v", handleErr)
				continue
			}

			if err = c.PushResponse(response); err != nil {
				c.log.Errorf("Error while pushing response: %v", err)
				continue
			}

			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				c.log.Errorf("Error while committing consumer: %v", commitErr)
			}
		}
	}
}

func (c *KafkaConsumer) PushResponse(response *messages.MessageResponse) error {
	jsonData, err := json.Marshal(response)
	if err != nil {
		return err
	}
	keyUUID, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(keyUUID.String()),
		Value: jsonData,
		Time:  time.Now(),
	}
	err = c.writer.WriteMessages(context.Background(), msg)
	if err != nil {
		return err
	}
	return nil
}

func (c *KafkaConsumer) Stop() {
	err := c.writer.Close()
	if err != nil {
		c.log.Errorf("Error while closing kafka writer: %v", err)
	}
}
