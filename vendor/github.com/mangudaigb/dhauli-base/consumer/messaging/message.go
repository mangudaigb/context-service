package messaging

import (
	"encoding/json"
)

// Message
// Type is the entity on which the message is acting on.
// For example, a message to create a user is of type User.
// The message payload is the user data.
// Action is the operation that is being performed on the entity.
// For example, a message to create a user is of action Create.
type Message struct {
	ID             string          `json:"id,omitempty"`
	Version        int             `json:"version,omitempty"`
	WorkflowId     string          `json:"workflowId,omitempty"`
	SessionId      string          `json:"sessionId,omitempty"`
	ConversationId string          `json:"conversationId,omitempty"`
	InteractionId  string          `json:"interactionId,omitempty"`
	Type           Type            `json:"type,omitempty"`
	Action         Action          `json:"action,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
	Metadata       map[string]any  `json:"metadata,omitempty"`
}

type ErrorData struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	SkipRetry bool   `json:"skipRetry"`
}

func (m *Message) DecodeData(target any) error {
	if len(m.Data) == 0 {
		return nil
	}
	return json.Unmarshal(m.Data, target)
}
