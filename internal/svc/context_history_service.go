package svc

import (
	"context"
	"time"

	"github.com/mangudaigb/context-service/internal/repo"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ContextHistoryService interface {
	GetContextHistoryByID(ctx context.Context, id string) (*entities.ContextHistory, error)
	AddHistoryForContext(ctx context.Context, c *entities.Context) (*entities.ContextHistory, error)
	GetHistoryForContextId(ctx context.Context, cid string) ([]*entities.ContextHistory, error)
}

type contextHistoryService struct {
	log                      *logger.Logger
	contextHistoryRepository repo.ContextHistoryRepository
}

func NewContextHistoryService(log *logger.Logger, repo repo.ContextHistoryRepository) ContextHistoryService {
	return &contextHistoryService{
		log:                      log,
		contextHistoryRepository: repo,
	}
}

func (chs contextHistoryService) GetContextHistoryByID(ctx context.Context, id string) (*entities.ContextHistory, error) {
	return chs.contextHistoryRepository.GetByID(ctx, id)
}

func (chs contextHistoryService) AddHistoryForContext(ctx context.Context, c *entities.Context) (*entities.ContextHistory, error) {
	ch := &entities.ContextHistory{
		ID:            primitive.NewObjectID().Hex(),
		ContextID:     c.ID,
		Name:          c.Name,
		Description:   c.Description,
		Content:       c.Content,
		Organizations: c.Organizations,
		Tenants:       c.Tenants,
		Groups:        c.Groups,
		User:          c.User,
		CreatedTime:   time.Now(),
		IsActive:      c.IsActive,
		Version:       c.Version,
		Tags:          c.Tags,
		Metadata:      c.Metadata,
	}
	create, err := chs.contextHistoryRepository.Create(ctx, ch)
	if err != nil {
		chs.log.Errorf("Error creating context history: %v for id: %s", err, c.ID)
		return nil, err
	}
	return create, nil
}

func (chs contextHistoryService) GetHistoryForContextId(ctx context.Context, cid string) ([]*entities.ContextHistory, error) {
	filter := bson.M{"context_id": cid}
	list, err := chs.contextHistoryRepository.Filter(ctx, filter)
	if err != nil {
		chs.log.Errorf("Error getting history for context id: %s with err: %v", cid, err)
		return nil, err
	}
	return list, nil
}
