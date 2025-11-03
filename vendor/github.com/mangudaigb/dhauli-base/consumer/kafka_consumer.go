package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/consumer/messaging"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type Handler func(ctx context.Context, envelope *messaging.Envelope) *messaging.Envelope

type KafkaConsumer struct {
	name    string
	topic   string
	log     *logger.Logger
	handler Handler
	reader  *kafka.Reader
	writer  *kafka.Writer
	tr      trace.Tracer
}

func NewKafkaConsumer(cfg *config.Config, tracer trace.Tracer, logger *logger.Logger, handler Handler) *KafkaConsumer {
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

	consumerName := cfg.Server.Name
	topic := cfg.Kafka.Topic

	return &KafkaConsumer{
		name:    consumerName,
		topic:   topic,
		log:     logger,
		handler: handler,
		reader:  kReader,
		writer:  &kWriter,
		tr:      tracer,
	}
}

func (c *KafkaConsumer) Consume(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			c.log.Errorf("Context cancelled. Closing kafka reader...")
			return c.reader.Close()

		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					c.log.Errorf("Fetch consumer cancelled due to context cancellation: %v", err)
					continue
				}
				c.log.Errorf("Error while fetching consumer: %v", err)
				c.handleError(ctx, nil, err, false, 3*time.Second)
				continue
			}

			traceCtx := c.extractTraceContext(ctx, &msg)
			spanCtx, span := c.tr.Start(
				traceCtx,
				fmt.Sprintf("%s process", c.topic),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String("kafka"),
					semconv.MessagingDestinationNameKey.String(msg.Topic),
					semconv.MessagingOperationReceive,
					attribute.Int64("message.offset", msg.Offset),
					attribute.Int("message.partition", msg.Partition),
				),
			)
			defer span.End()

			reqEnvelope, err := messaging.FromJSON(msg.Value)
			// TODO If I cannot parse the envelope, I cannot know session id, so I will just ignore the message. The router has to have a timeout for this.
			if err != nil {
				c.log.Errorf("Error while unmarshalling message: %v", err)
				c.handleError(spanCtx, &msg, err, true, 0)
				continue
			}

			span.SetAttributes(
				attribute.String("envelope.id", reqEnvelope.ID),
				attribute.String("envelope.schema_version", reqEnvelope.SchemaVersion),
				attribute.String("envelope.event_name", string(reqEnvelope.EventName)),
				attribute.String("envelope.correlation_id", reqEnvelope.CorrelationId),
				attribute.Int("envelope.retry_count", reqEnvelope.RetryCount),
				attribute.Int("envelope.max_retries", reqEnvelope.MaxRetries),
			)

			envelope := c.handler(spanCtx, &reqEnvelope)

			// TODO Ideally idempotent actions behavior should be different from non-idempotent actions.
			if err = c.pushResponse(envelope); err != nil {
				c.log.Errorf("Error while pushing response: %v", err)
				c.handleError(spanCtx, &msg, err, true, 0)
				continue
			}

			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				c.log.Errorf("Error while committing consumer: %v", commitErr)
				c.handleError(spanCtx, &msg, commitErr, true, 0)
				continue
			}
		}
	}
}

func (c *KafkaConsumer) extractTraceContext(ctx context.Context, msg *kafka.Message) context.Context {
	headers := msg.Headers
	carrier := propagation.MapCarrier{}
	for _, header := range headers {
		carrier[header.Key] = string(header.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func (c *KafkaConsumer) handleError(ctx context.Context, msg *kafka.Message, err error, commit bool, sleep time.Duration) {
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	if commit && msg != nil {
		if commitErr := c.reader.CommitMessages(ctx, *msg); commitErr != nil {
			c.log.Errorf("Error committing offset: %v", commitErr)
		}
	}
	if sleep > 0 {
		time.Sleep(sleep)
	}
}

func (c *KafkaConsumer) pushResponse(envelope *messaging.Envelope) error {
	c.log.Infof("Pushing response to kafka: %v", envelope)
	jsonData, err := envelope.ToJSON()
	if err != nil {
		c.log.Errorf("Error while marshalling envelope: %v", err)
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
		c.log.Errorf("Error while writing message to kafka: %v", err)
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
