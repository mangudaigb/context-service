package internal

import (
	"context"
	"errors"
	"time"

	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrContextNotFound        = errors.New("context not found")
	ErrContextVersionMismatch = errors.New("version conflict")
)

type ContextRepository interface {
	GetByID(ctx context.Context, id string) (*entities.Context, error)
	Create(ctx context.Context, c *entities.Context) (*entities.Context, error)
	Update(ctx context.Context, newContext *entities.Context) (*entities.Context, error)
	Delete(ctx context.Context, id string) error
	Filter(ctx context.Context, filter interface{}) ([]*entities.Context, error)
	Close()
}

type MongoContextRepository struct {
	log        *logger.Logger
	collection *mongo.Collection
}

func NewContextRepository(cfg *config.Config, log *logger.Logger, client mongo.Client, collection string) ContextRepository {
	col := client.Database(cfg.Mongo.Database).Collection(collection)
	return &MongoContextRepository{
		collection: col,
		log:        log,
	}
}

func (mcr *MongoContextRepository) GetByID(ctx context.Context, id string) (*entities.Context, error) {
	contextDoc := &entities.Context{}
	filter := bson.M{"_id": id}
	err := mcr.collection.FindOne(ctx, filter).Decode(&contextDoc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrContextNotFound
	}
	return contextDoc, nil
}

func (mcr *MongoContextRepository) Create(ctx context.Context, c *entities.Context) (*entities.Context, error) {
	_, err := mcr.collection.InsertOne(ctx, c)
	if err != nil {
		mcr.log.Errorf("Error inserting context: %v", err)
		return nil, err
	}
	return mcr.GetByID(ctx, c.ID)
}

func (mcr *MongoContextRepository) Update(ctx context.Context, nc *entities.Context) (*entities.Context, error) {
	filter := bson.M{
		"_id":     nc.ID,
		"version": nc.Version,
	}
	nc.Version++
	nc.ModifiedTime = time.Now().UTC()
	update := bson.M{
		"$set": bson.M{
			"name":          nc.Name,
			"description":   nc.Description,
			"content":       nc.Content,
			"organizations": nc.Organizations,
			"tenants":       nc.Tenants,
			"groups":        nc.Groups,
			"user":          nc.User,
			"modifiedTime":  nc.ModifiedTime,
			"isActive":      nc.IsActive,
			"version":       nc.Version,
			"tags":          nc.Tags,
			"metadata":      nc.Metadata,
		},
	}

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var updatedContext entities.Context
	err := mcr.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&updatedContext)
	if errors.Is(err, mongo.ErrNoDocuments) {
		mcr.log.Errorf("Error updating context in mongo: %v", err)
		return nil, ErrContextVersionMismatch
	}
	return &updatedContext, nil
}

func (mcr *MongoContextRepository) Delete(ctx context.Context, id string) error {
	res, err := mcr.collection.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return ErrContextNotFound
	}
	return nil
}

func (mcr *MongoContextRepository) Filter(ctx context.Context, filter interface{}) ([]*entities.Context, error) {
	cursor, err := mcr.collection.Find(ctx, filter)
	if err != nil {
		mcr.log.Errorf("Error finding documents: %v", err)
		return nil, err
	}
	defer func() {
		if closeErr := cursor.Close(ctx); closeErr != nil {
			mcr.log.Errorf("Error closing context cursor: %v", closeErr)
		}
	}()

	var contexts []*entities.Context
	if err = cursor.All(ctx, &contexts); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return []*entities.Context{}, nil
		}
		mcr.log.Errorf("Error decoding documents: %v", err)
		return nil, err
	}
	if err := cursor.Err(); err != nil {
		mcr.log.Errorf("Error iterating cursor: %v", err)
		return nil, err
	}

	return contexts, nil
}

func (mcr *MongoContextRepository) Close() {
	err := mcr.collection.Database().Client().Disconnect(context.Background())
	if err != nil {
		mcr.log.Errorf("Error closing mongo client for context: %v", err)
		return
	}
}
