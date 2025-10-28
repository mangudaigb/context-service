package message

type MessageWorkflow struct {
	Id     string `json:"id"`
	StepId string `json:"stepId"`
}

type Payload struct {
	Action string                 `json:"action"`
	Data   map[string]interface{} `json:"data"`
}

type MessageRequest struct {
	Id       string                 `json:"id"`
	StateId  string                 `json:"stateId"`
	Workflow MessageWorkflow        `json:"workflow"`
	Payload  Payload                `json:"payload"`
	Trace    map[string]interface{} `json:"trace"`
}

type MessageResponse struct {
	Status   int                    `json:"status"`
	ErrorMsg string                 `json:"error_msg"`
	Workflow MessageWorkflow        `json:"workflow"`
	Response map[string]interface{} `json:"response"`
	Trace    map[string]interface{} `json:"trace"`
}
