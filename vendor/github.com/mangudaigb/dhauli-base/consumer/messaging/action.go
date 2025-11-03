package messaging

type Action string

const (
	GET    Action = "get"
	CREATE Action = "create"
	UPDATE Action = "update"
	DELETE Action = "delete"
)
