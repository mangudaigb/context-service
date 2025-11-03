package entities

import "time"

type Memento struct {
	Index int    `json:"index"`
	State string `json:"state"`
	Actor string `json:"actor"`
}

type Interaction struct {
	ID             string    `json:"id" bson:"_id,omitempty"`
	WorkflowID     string    `json:"workflowId" bson:"workflowId"`
	SessionID      string    `json:"sessionId" bson:"sessionId"`
	ConversationID string    `json:"conversationId" bson:"conversationId"`
	Context        string    `json:"context" bson:"context"`
	Query          string    `json:"query" bson:"query"`
	Answer         string    `json:"answer" bson:"answer"`
	CreatedAt      time.Time `json:"createdAt" bson:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt" bson:"updatedAt"`
	Version        int       `json:"version" bson:"version"`
}

type InteractionHistory struct {
	ID             string    `json:"id" bson:"_id,omitempty"`
	WorkflowID     string    `json:"workflowId" bson:"workflowId"`
	SessionID      string    `json:"sessionId" bson:"sessionId"`
	ConversationID string    `json:"conversationId" bson:"conversationId"`
	InteractionID  string    `json:"interactionId" bson:"interactionId"`
	Action         string    `json:"action" bson:"action"`
	Actor          string    `json:"actor" bson:"actor"`
	Context        string    `json:"context" bson:"context"`
	Query          string    `json:"query" bson:"query"`
	Answer         string    `json:"answer" bson:"answer"`
	CreatedAt      time.Time `json:"createdAt" bson:"createdAt"`
	Version        int       `json:"version" bson:"version"`
}

type Conversation struct {
	ID           string            `json:"id" bson:"_id,omitempty"`
	WorkflowID   string            `json:"workflowId" bson:"workflowId"`
	SessionID    string            `json:"sessionId" bson:"sessionId"`
	UserID       string            `json:"userId,omitempty" bson:"userId,omitempty"`
	Interactions []InteractionStub `json:"interactions" bson:"interactions"`
	CreatedAt    time.Time         `json:"createdAt" bson:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt" bson:"updatedAt"`
	Version      int               `json:"version" bson:"version"`
}

type InteractionStub struct {
	ID     string `json:"id" bson:"_id,omitempty"`
	Query  string `json:"query,omitempty" bson:"query"`
	Answer string `json:"answer,omitempty" bson:"answer"`
}
