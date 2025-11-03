package messaging

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func NewMessageFromOld(oldMessage Message, msgType Type, action Action, data any) (Message, error) {
	var raw json.RawMessage
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return Message{}, err
		}
		raw = b
	}
	return Message{
		ID:             uuid.NewString(),
		Version:        1,
		WorkflowId:     oldMessage.WorkflowId,
		SessionId:      oldMessage.SessionId,
		ConversationId: oldMessage.ConversationId,
		InteractionId:  oldMessage.InteractionId,
		Type:           msgType,
		Action:         action,
		Data:           raw,
		Metadata:       oldMessage.Metadata,
	}, nil
}

func NewEnvelope(message Message, opts ...EnvelopeOption) Envelope {
	e := Envelope{
		ID:             message.ID,
		SchemaVersion:  "1.0",
		CorrelationId:  uuid.NewString(),
		TraceId:        uuid.NewString(),
		IdempotencyKey: uuid.NewString(),
		Kind:           REQUEST,
		EventName:      "",
		RetryCount:     0,
		MaxRetries:     0,
		CreatedAt:      time.Now(),
		Message:        message,
	}
	for _, opt := range opts {
		opt(&e)
	}
	return e
}

// Handles enveloping errors which if fatal are skipped.
// Usecase: When the mesage json is malformed, the error is fatal.
func EnvelopeError(env Envelope, eventName EventName, skipRetry bool) *Envelope {
	retryCount := env.RetryCount
	if skipRetry {
		retryCount = env.MaxRetries
	} else {
		retryCount++
	}
	response := NewEnvelope(
		env.Message,
		WithKind(ERROR),
		WithEventName(eventName),
		WithCorrelationId(env.CorrelationId),
		WithRetryCount(retryCount),
	)
	return &response
}

func MessageError(env *Envelope, code int, err error, skipRetry bool) *Envelope {
	errorData := ErrorData{
		Code:      code,
		Message:   err.Error(),
		SkipRetry: skipRetry,
	}
	message, err := NewMessageFromOld(env.Message, env.Message.Type, env.Message.Action, errorData)
	var response Envelope
	if err != nil { // This should never happen.
		response = NewEnvelope(
			Message{},
			WithKind(ERROR),
			WithEventName("fatal-error"),
			WithCorrelationId(env.CorrelationId),
			WithRetryCount(env.RetryCount+1),
		)
	} else {
		response = NewEnvelope(
			message,
			WithKind(RESPONSE),
			WithEventName("error"),
			WithCorrelationId(env.CorrelationId),
			WithRetryCount(env.RetryCount+1),
		)
	}
	return &response
}

func FromJSON(data []byte) (Envelope, error) {
	var env Envelope
	err := json.Unmarshal(data, &env)
	return env, err
}
