package internal

import (
	"context"
	"encoding/json"

	"github.com/mangudaigb/dhauli-base/logger"
	messages "github.com/mangudaigb/dhauli-base/types/message"
	"github.com/segmentio/kafka-go"
)

func MsgHandlerFunc(ctx context.Context, log *logger.Logger, msg kafka.Message) (*messages.MessageResponse, error) {
	var incomingMsg messages.MessageRequest
	if err := json.Unmarshal(msg.Value, &incomingMsg); err != nil {
		log.Errorf("Error unmarshalling message: %v", err)
		return nil, err
	}
	action := incomingMsg.Payload.Action
	data := incomingMsg.Payload.Data
	log.Infof("Received message action: %s", action)
	log.Infof("Received message data: %v", data)

	return nil, nil
}
