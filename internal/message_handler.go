package internal

import (
	"context"
	"errors"

	"github.com/mangudaigb/context-service/internal/consumer"
	"github.com/mangudaigb/dhauli-base/consumer/messaging"
	"github.com/mangudaigb/dhauli-base/logger"
	"go.opentelemetry.io/otel/trace"
)

type MessageHandler struct {
	tr          trace.Tracer
	log         *logger.Logger
	cMsgHandler *consumer.ContextMsgHandler
}

func (mh *MessageHandler) HandlerFunc(ctx context.Context, envelope *messaging.Envelope) *messaging.Envelope {
	_, span := mh.tr.Start(ctx, "message_handler")
	defer span.End()
	mh.log.Infof("Received message: %s", envelope.Message.Action)

	kind := envelope.Kind
	//event := envelope.EventName
	message := envelope.Message
	mType := message.Type
	mAction := message.Action

	if kind != messaging.REQUEST {
		mh.log.Errorf("Invalid message kind: %s. Expected REQUEST.", kind)
		return messaging.EnvelopeError(*envelope, "kind is not of type request", true)
	}

	if mType != "context" {
		ictxt, err := mh.cMsgHandler.MsgHandlerFunc(ctx, message, mAction)
		if err != nil {
			mh.log.Errorf("Error handling message: %v", err)
			return messaging.MessageError(envelope, 500, errors.New("context handler error"), false)
		}
		responseMsg, err := messaging.NewMessageFromOld(message, "context", message.Action, ictxt)
		if err != nil {
			mh.log.Errorf("Error creating response message: %v", err)
			return messaging.MessageError(envelope, 500, errors.New("error creating response message"), false)
		}
		responseEnv := messaging.NewEnvelope(
			responseMsg,
			messaging.WithCorrelationId(envelope.CorrelationId),
			messaging.WithTraceId(envelope.TraceId),
			messaging.WithIdempotencyKey(envelope.IdempotencyKey),
			messaging.WithKind(messaging.RESPONSE),
			messaging.WithEventName("success"),
		)
		return &responseEnv
	} else {
		mh.log.Errorf("Invalid message Type: %s. Expected context.", mType)
		return messaging.EnvelopeError(*envelope, "type is not of type context", true)
	}
}

func NewMessageHandler(tr trace.Tracer, log *logger.Logger, cHandler *consumer.ContextMsgHandler) *MessageHandler {
	return &MessageHandler{
		tr:          tr,
		log:         log,
		cMsgHandler: cHandler,
	}
}
