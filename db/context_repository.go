package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/db"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/types/entities"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ContextRepository interface {
	GetContextByID(ctx context.Context, id string) (*entities.Context, error)
	CreateContext(ctx context.Context, c *entities.Context) (*entities.Context, error)
	UpdateContext(ctx context.Context, id string, patch bson.M) (*entities.Context, error)
	DeleteContext(ctx context.Context, id string) error
	Close()
}

type MongoContextRepository struct {
	client               *db.MongoClient
	contextCollection    *mongo.Collection
	ctxVersionCollection *mongo.Collection
	log                  *logger.Logger
}

func NewContextRepository(cfg *config.Config, log *logger.Logger, collection string) ContextRepository {
	mongoClient, err := db.NewMongoClient(cfg, log)
	if err != nil {
		log.Errorf("Error creating mongo client: %v", err)
		panic(err)
	}

	col := mongoClient.Client.Database(cfg.Mongo.Database).Collection(collection)

	return &MongoContextRepository{
		client:            mongoClient,
		contextCollection: col,
		log:               log,
	}
}

func NewContextVersion(ctx entities.Context) *entities.ContextVersion {
	return &entities.ContextVersion{
		ContextID:     ctx.ID,
		Name:          ctx.Name,
		Description:   ctx.Description,
		Content:       ctx.Content,
		Organizations: ctx.Organizations,
		Tenants:       ctx.Tenants,
		Groups:        ctx.Groups,
		User:          ctx.User,
		CreatedTime:   ctx.CreatedTime,
		ModifiedTime:  ctx.ModifiedTime,
		IsActive:      ctx.IsActive,
		Version:       ctx.Version,
		Tags:          ctx.Tags,
		Metadata:      ctx.Metadata,
	}
}

func (mcr *MongoContextRepository) GetContextByID(ctx context.Context, id string) (*entities.Context, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, errors.New("invalid Context ID format")
	}

	filter := bson.M{"_id": oid}
	var contextDoc entities.Context

	err = mcr.contextCollection.FindOne(ctx, filter).Decode(&contextDoc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	return &contextDoc, nil
}

func (mcr *MongoContextRepository) CreateContext(ctx context.Context, c *entities.Context) (*entities.Context, error) {
	now := time.Now()
	c.CreatedTime = now
	c.ModifiedTime = now
	result, err := mcr.contextCollection.InsertOne(ctx, c)
	if err != nil {
		mcr.log.Errorf("Error inserting context: %v", err)
		return nil, err
	}

	if oid, ok := result.InsertedID.(primitive.ObjectID); ok {
		c.ID = oid.Hex()
	}

	return c, nil
}

func (mcr *MongoContextRepository) UpdateContext(ctx context.Context, id string, updates bson.M) (*entities.Context, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		mcr.log.Errorf("Error updating context: %v", err)
		return nil, errors.New("invalid Context ID format")
	}

	expectedVersion, ok := updates["version"].(int)
	if !ok {
		mcr.log.Errorf("optimistic locking failed: 'version' (int) must be included in updates: %v", err)
		return nil, errors.New("optimistic locking failed: 'version' (int) must be included in updates")
	}
	delete(updates, "version")

	filter := bson.M{
		"_id":     oid,
		"version": expectedVersion,
	}

	var oldContext entities.Context
	err = mcr.contextCollection.FindOne(ctx, filter).Decode(&oldContext)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			mcr.log.Errorf("lookup failed: document for id not found for update: %s %v", id, err)
			return nil, errors.New("lookup failed: document for id not found for update")
		}
		return nil, err
	}

	versionDoc := NewContextVersion(oldContext)
	if _, err := mcr.ctxVersionCollection.InsertOne(ctx, versionDoc); err != nil {
		return nil, fmt.Errorf("failed to save version history for ContextID %s (Version %d): %w", id, oldContext.Version, err)
	}

	updateDoc := bson.M{
		"$set":         updates,
		"$currentDate": bson.M{"modifiedTime": true},
		"$inc":         bson.M{"version": 1},
	}

	delete(updateDoc["$set"].(bson.M), "modifiedTime")

	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var updatedContext entities.Context

	result := mcr.contextCollection.FindOneAndUpdate(ctx, filter, updateDoc, opts)
	err = result.Decode(&updatedContext)

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("document not found for update")
		}
		return nil, err
	}

	return &updatedContext, nil
}

func (mcr *MongoContextRepository) DeleteContext(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return errors.New("invalid Context ID format")
	}

	filter := bson.M{"_id": oid}

	res, err := mcr.contextCollection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}

	if res.DeletedCount == 0 {
		return errors.New("document not found for deletion")
	}

	return nil
}

func (mcr *MongoContextRepository) Close() {
	err := mcr.contextCollection.Database().Client().Disconnect(context.Background())
	if err != nil {
		mcr.log.Errorf("Error closing mongo client for context: %v", err)
		return
	}
}
