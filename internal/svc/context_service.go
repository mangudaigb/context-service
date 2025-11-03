package svc

import (
	"context"
	"errors"
	"time"

	"github.com/mangudaigb/context-service/internal/repo"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
)

var (
	ErrInvalidInput = errors.New("invalid input")
)

type ContextService interface {
	CreateContext(ctx context.Context, c *entities.Context) (*entities.Context, error)
	GetContextByID(ctx context.Context, id string) (*entities.Context, error)
	UpdateContext(ctx context.Context, c *entities.Context) (*entities.Context, error)
	DeleteContext(ctx context.Context, id string) (*entities.Context, error)
	FilterContexts(ctx context.Context, filter interface{}) ([]*entities.Context, error)
}

type contextService struct {
	log                   *logger.Logger
	contextHistoryService ContextHistoryService
	contextRepository     repo.ContextRepository
}

func NewContextService(log *logger.Logger, repo repo.ContextRepository, chs ContextHistoryService) ContextService {
	return &contextService{
		log:                   log,
		contextRepository:     repo,
		contextHistoryService: chs,
	}
}

func (cs contextService) CreateContext(ctx context.Context, c *entities.Context) (*entities.Context, error) {
	if c.ID == "" {
		cs.log.Errorf("Invalid input: context ID is empty")
		return nil, ErrInvalidInput
	}
	c.IsActive = true
	c.Version = 1
	c.CreatedTime = time.Now()
	c.ModifiedTime = time.Now()
	createdContext, err := cs.contextRepository.Create(ctx, c)
	if err != nil {
		return nil, err
	}
	return createdContext, nil
}

func (cs contextService) GetContextByID(ctx context.Context, id string) (*entities.Context, error) {
	c, err := cs.contextRepository.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (cs contextService) UpdateContext(ctx context.Context, c *entities.Context) (*entities.Context, error) {
	oc, err := cs.contextRepository.GetByID(ctx, c.ID)
	if err != nil {
		cs.log.Errorf("Error getting context for ID: %s with err: %v", c.ID, err)
		return nil, err
	}
	_, err = cs.contextHistoryService.AddHistoryForContext(ctx, oc)
	if err != nil {
		cs.log.Errorf("Error adding history for context: %v", err)
	}

	nc, err := cs.contextRepository.Update(ctx, c)
	if err != nil {
		cs.log.Errorf("Error updating context: %v", err)
		return nil, err
	}
	return nc, nil
}

func (cs contextService) DeleteContext(ctx context.Context, id string) (*entities.Context, error) {
	c, err := cs.contextRepository.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	c.IsActive = false
	uc, err := cs.contextRepository.Update(ctx, c)
	if err != nil {
		return nil, err
	}
	return uc, nil
}

func (cs contextService) FilterContexts(ctx context.Context, filter interface{}) ([]*entities.Context, error) {
	contexts, err := cs.contextRepository.Filter(ctx, filter)
	if err != nil {
		cs.log.Errorf("Error filtering contexts: %v", err)
		return nil, err
	}
	return contexts, nil
}
