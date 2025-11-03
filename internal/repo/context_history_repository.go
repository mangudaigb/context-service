package repo

import (
	"context"
	"errors"
	"time"

	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ErrContextHistoryNotFound        = errors.New("context history not found")
	ErrContextHistoryVersionMismatch = errors.New("context history version conflict")
)

type ContextHistoryRepository interface {
	GetByID(ctx context.Context, id string) (*entities.ContextHistory, error)
	Create(ctx context.Context, c *entities.ContextHistory) (*entities.ContextHistory, error)
	Update(ctx context.Context, newContext *entities.ContextHistory) (*entities.ContextHistory, error)
	Delete(ctx context.Context, id string) error
	Filter(ctx context.Context, filter interface{}) ([]*entities.ContextHistory, error)
	Close()
}

type MongoContextHistoryRepository struct {
	log        *logger.Logger
	collection *mongo.Collection
}

func NewContextHistoryRepository(cfg *config.Config, log *logger.Logger, client mongo.Client, collection string) ContextHistoryRepository {
	col := client.Database(cfg.Mongo.Database).Collection(collection)
	return &MongoContextHistoryRepository{
		collection: col,
		log:        log,
	}
}

func (m MongoContextHistoryRepository) GetByID(ctx context.Context, id string) (*entities.ContextHistory, error) {
	contextHistoryDoc := &entities.ContextHistory{}
	filter := bson.M{"_id": id}
	err := m.collection.FindOne(ctx, filter).Decode(&contextHistoryDoc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrContextHistoryNotFound
	}
	return contextHistoryDoc, nil
}

func (m MongoContextHistoryRepository) Create(ctx context.Context, ch *entities.ContextHistory) (*entities.ContextHistory, error) {
	now := time.Now()
	ch.CreatedTime = now
	_, err := m.collection.InsertOne(ctx, ch)
	if err != nil {
		m.log.Errorf("Error inserting context history: %v", err)
		return nil, err
	}
	return ch, nil
}

func (m MongoContextHistoryRepository) Update(ctx context.Context, newContext *entities.ContextHistory) (*entities.ContextHistory, error) {
	//TODO implement me
	panic("no need for this")
}

func (m MongoContextHistoryRepository) Delete(ctx context.Context, id string) error {
	//TODO implement me
	panic("no need for this")
}

func (m MongoContextHistoryRepository) Filter(ctx context.Context, filter interface{}) ([]*entities.ContextHistory, error) {
	cursor, err := m.collection.Find(ctx, filter)
	if err != nil {
		m.log.Errorf("Error finding documents: %v", err)
		return nil, err
	}
	defer func() {
		if closeErr := cursor.Close(ctx); closeErr != nil {
			m.log.Errorf("Error closing context history cursor: %v", closeErr)
		}
	}()
	var contextHistories []*entities.ContextHistory
	if err = cursor.All(ctx, &contextHistories); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return []*entities.ContextHistory{}, nil
		}
		m.log.Errorf("Error decoding documents: %v", err)
		return nil, err
	}
	if err := cursor.Err(); err != nil {
		m.log.Errorf("Error iterating cursor: %v", err)
		return nil, err
	}
	return contextHistories, nil
}

func (m MongoContextHistoryRepository) Close() {
	err := m.collection.Database().Client().Disconnect(context.Background())
	if err != nil {
		m.log.Errorf("Error closing mongo client for context history: %v", err)
		return
	}
}
