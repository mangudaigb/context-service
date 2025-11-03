package consumer

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mangudaigb/context-service/internal/svc"
	"github.com/mangudaigb/context-service/pkg/requests"
	"github.com/mangudaigb/dhauli-base/consumer/messaging"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.opentelemetry.io/otel/trace"
)

type ContextMsgHandler struct {
	tr   trace.Tracer
	log  *logger.Logger
	cSvc svc.ContextService
}

func (cmh *ContextMsgHandler) MsgHandlerFunc(ctx context.Context, message messaging.Message, action messaging.Action) (*entities.Context, error) {
	var err error
	var out *entities.Context
	if action == "create" {
		out, err = cmh.handleCreate(ctx, message)
	} else if action == "update" {
		out, err = cmh.handleUpdate(ctx, message)
	} else if action == "delete" {
		out, err = cmh.handleDelete(ctx, message)
	} else {
		cmh.log.Errorf("Invalid action: %s", action)
		return nil, errors.New("invalid action")
	}
	return out, err
}

func (cmh *ContextMsgHandler) handleCreate(ctx context.Context, msg messaging.Message) (*entities.Context, error) {
	var req requests.ContextRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		cmh.log.Errorf("Error unmarshalling message: %v", err)
		return nil, err
	}
	c := &entities.Context{
		ID:            primitive.NewObjectID().Hex(),
		Name:          req.Name,
		Description:   req.Description,
		Content:       req.Content,
		Organizations: nil,
		Tenants:       nil,
		Groups:        nil,
		User:          entities.UserStub{},
	}
	createdContext, err := cmh.cSvc.CreateContext(ctx, c)
	if err != nil {
		cmh.log.Errorf("Error creating context: %v", err)
		return nil, err
	}
	return createdContext, nil
}

func (cmh *ContextMsgHandler) handleUpdate(ctx context.Context, msg messaging.Message) (*entities.Context, error) {
	var update entities.Context
	if err := json.Unmarshal(msg.Data, &update); err != nil {
		cmh.log.Errorf("Error unmarshalling message: %v", err)
	}

	if update.ID == "" {
		cmh.log.Errorf("Context Id is required to update context")
		return nil, errors.New("context Id is required to update context")
	}

	updatedContext, err := cmh.cSvc.UpdateContext(ctx, &update)
	if err != nil {
		cmh.log.Errorf("Error updating context: %v", err)
		return nil, err
	}
	return updatedContext, nil
}

func (cmh *ContextMsgHandler) handleDelete(ctx context.Context, msg messaging.Message) (*entities.Context, error) {
	var req requests.ContextRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		cmh.log.Errorf("Error unmarshalling message: %v", err)
	}

	// TODO do not see the need for it yet

	return nil, nil
}

func NewContextMsgHandler(tr trace.Tracer, log *logger.Logger, cSvc svc.ContextService) *ContextMsgHandler {
	return &ContextMsgHandler{
		tr:   tr,
		log:  log,
		cSvc: cSvc,
	}
}
