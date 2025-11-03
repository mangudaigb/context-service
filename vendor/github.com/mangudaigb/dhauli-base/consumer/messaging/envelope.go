package messaging

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/mangudaigb/dhauli-base/types/entities"
)

type AuthInfo struct {
	Token   string `json:"token,omitempty"`
	Subject string `json:"subject,omitempty"`
	Issuer  string `json:"issuer,omitempty"`
	Scopes  string `json:"scopes,omitempty"`
}

type Principal struct {
	Organization entities.OrganizationStub `json:"organization,omitempty"`
	Tenant       entities.TenantStub       `json:"tenant,omitempty"`
	Group        entities.GroupStub        `json:"group,omitempty"`
	User         entities.UserStub         `json:"user,omitempty"`
}

// Envelope
// Kind is an event, command, query, error, request, response
// This identifies the behavior of the message. For example, an event is kind of notification
// whereas a command is something for the cconsumer to act on.
// EventName specifies the semantic event, like UserCreated, UserUpdated, etc.
// ┌────────────────────────────┐
// │        Envelope            │
// │                            │
// │ Kind: Event                │  ← messaging-level
// │ EventName: UserUpdated     │
// │ TraceId, CorrelationId ... │
// │                            │
// │   ┌────────────────────┐   │
// │   │     Message        │   │
// │   │                    │   │
// │   │ Type: Conversation │   │  ← domain-level
// │   │ Action: Updated    │   │
// │   │ Data: {...}        │   │
// │   └────────────────────┘   │
// └────────────────────────────┘
type Envelope struct {
	ID             string     `json:"id"`
	SchemaVersion  string     `json:"schemaVersion"`
	CorrelationId  string     `json:"correlationId,omitempty"`
	IdempotencyKey string     `json:"idempotencyKey,omitempty"`
	Kind           Kind       `json:"kind,omitempty"`
	EventName      EventName  `json:"eventName,omitempty"`
	RetryCount     int        `json:"retryCount"`
	MaxRetries     int        `json:"maxRetries"`
	TraceId        string     `json:"traceId"`
	CreatedAt      time.Time  `json:"createdAt"`
	AuthInfo       *AuthInfo  `json:"authInfo,omitempty"`
	Principal      *Principal `json:"principal,omitempty"`
	Message        Message    `json:"message"`
}

func (e *Envelope) WithRetry() Envelope {
	newEnv := *e
	newEnv.RetryCount++
	newEnv.ID = uuid.NewString()
	newEnv.CreatedAt = time.Now().UTC()
	return newEnv
}

func (e *Envelope) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

type EnvelopeOption func(*Envelope)

func WithAuthInfo(auth *AuthInfo) EnvelopeOption {
	return func(e *Envelope) {
		e.AuthInfo = auth
	}
}

func WithPrincipal(p *Principal) EnvelopeOption {
	return func(e *Envelope) {
		e.Principal = p
	}
}

func WithCorrelationId(id string) EnvelopeOption {
	return func(e *Envelope) {
		e.CorrelationId = id
	}
}

func WithIdempotencyKey(key string) EnvelopeOption {
	return func(e *Envelope) {
		e.IdempotencyKey = key
	}
}

func WithKind(kind Kind) EnvelopeOption {
	return func(e *Envelope) {
		e.Kind = kind
	}
}

func WithEventName(event EventName) EnvelopeOption {
	return func(e *Envelope) {
		e.EventName = event
	}
}

func WithMaxRetries(n int) EnvelopeOption {
	return func(e *Envelope) {
		e.MaxRetries = n
	}
}

func WithRetryCount(n int) EnvelopeOption {
	return func(e *Envelope) {
		e.RetryCount = n
	}
}

func WithTraceId(trace string) EnvelopeOption {
	return func(e *Envelope) {
		e.TraceId = trace
	}
}
