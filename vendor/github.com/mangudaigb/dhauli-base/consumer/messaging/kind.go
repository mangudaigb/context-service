package messaging

type Kind string

const (
	REQUEST  Kind = "request"
	RESPONSE Kind = "response"
	EVENT    Kind = "event"
	COMMAND  Kind = "command"
	QUERY    Kind = "query"
	ERROR    Kind = "error"
)
