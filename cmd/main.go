package main

import (
	"context"
	"fmt"

	"github.com/mangudaigb/context-service/internal"
	"github.com/mangudaigb/context-service/internal/consumer"
	"github.com/mangudaigb/context-service/internal/repo"
	"github.com/mangudaigb/context-service/internal/svc"
	"github.com/mangudaigb/context-service/pkg"
	"github.com/mangudaigb/dhauli-base/config"
	consumer2 "github.com/mangudaigb/dhauli-base/consumer"
	"github.com/mangudaigb/dhauli-base/db"
	"github.com/mangudaigb/dhauli-base/discover"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/tracing"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	cfg, err := config.GetConfig()
	if err != nil {
		fmt.Println("Error reading the config file", err)
		panic(err)
	}

	log, err := logger.NewLogger(cfg)
	if err != nil {
		fmt.Println("Error creating logger", err)
		panic(err)
	}

	tp := tracing.InitTracerProvider(cfg, log)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Errorf("Error shutting down tracer provider: %v", err)
		}
	}()
	tr := tp.Tracer("conversation-memory")

	registry := discover.NewRegistryInfo(cfg, log)
	registry.Register(discover.SERVICE)

	StartConsumer(context.Background(), cfg, tr, log)

	server := pkg.NewContextServer(cfg, tr, log)
	server.Start()

}

func StartConsumer(ctx context.Context, cfg *config.Config, tr trace.Tracer, log *logger.Logger) {
	mongoClient, err := db.NewMongoClient(cfg, log)
	if err != nil {
		log.Fatalf("Error creating mongo client: %v", err)
	}

	var contextHistoryRepo = repo.NewContextHistoryRepository(cfg, log, *mongoClient.Client, "context_histories")
	var contextRepo = repo.NewContextRepository(cfg, log, *mongoClient.Client, "contexts")
	var contextHistorySvc = svc.NewContextHistoryService(log, contextHistoryRepo)
	var contextSvc = svc.NewContextService(log, contextRepo, contextHistorySvc)
	var contextMsgHandler = consumer.NewContextMsgHandler(tr, log, contextSvc)
	var msgHandler = internal.NewMessageHandler(tr, log, contextMsgHandler)

	log.Infof("Starting kafka consumer")
	csmr := consumer2.NewKafkaConsumer(cfg, tr, log, msgHandler.HandlerFunc)
	defer csmr.Stop()

	go func() {
		err := csmr.Consume(context.Background())
		if err != nil { // Should never happen
			log.Errorf("Error running the consumer: %v", err)
			return
		}
	}()
}
