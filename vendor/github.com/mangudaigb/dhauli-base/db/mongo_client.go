package db

import (
	"context"
	"time"

	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	log    *logger.Logger
	Client *mongo.Client
	Ctx    context.Context
	Cancel context.CancelFunc
}

func NewMongoClient(cfg *config.Config, log *logger.Logger) (*MongoClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI(cfg.Mongo.Uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Errorf("Error connecting to mongo: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Errorf("Error pinging mongo: %v", err)
	}

	log.Info("Connected to mongo")

	return &MongoClient{
		log:    log,
		Client: client,
		Ctx:    ctx,
		Cancel: cancel,
	}, nil
}

func (mc *MongoClient) Close() {
	if mc.Client != nil {
		if err := mc.Client.Disconnect(mc.Ctx); err != nil {
			mc.log.Error("Error disconnecting from mongo: %v", err)
		}
	}
	if mc.Cancel != nil {
		mc.Cancel()
	}
}
