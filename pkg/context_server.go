package pkg

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mangudaigb/context-service/internal/handler"
	"github.com/mangudaigb/context-service/internal/repo"
	"github.com/mangudaigb/context-service/internal/svc"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/consumer"
	"github.com/mangudaigb/dhauli-base/db"
	"github.com/mangudaigb/dhauli-base/logger"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/context"
)

type HttpServer struct {
	*consumer.KafkaConsumer
	zkClient *db.ZkClient
}

type ContextServer struct {
	log *logger.Logger
	cfg *config.Config
	tr  trace.Tracer
}

func NewContextServer(cfg *config.Config, tr trace.Tracer, log *logger.Logger) *ContextServer {
	return &ContextServer{
		log: log,
		cfg: cfg,
		tr:  tr,
	}
}

func SetupRouter(log *logger.Logger, cSvc svc.ContextService, chSvc svc.ContextHistoryService) *gin.Engine {
	r := gin.Default()
	cHandler := handler.NewContextHandler(log, cSvc)
	chHandler := handler.NewContextHistoryHandler(log, chSvc)

	contextRoutes := r.Group("/contexts")
	{
		contextRoutes.GET("/", cHandler.GetContextByFilter)
		contextRoutes.GET("/:cid", cHandler.GetContext)
		contextRoutes.POST("/", cHandler.CreateContext)
		contextRoutes.PATCH("/:cid", cHandler.UpdateContext) // Using PATCH for partial updates
		contextRoutes.DELETE("/:cid", cHandler.DeleteContext)

		contextHistoryRoutes := r.Group("/:cid/context-histories")
		{
			contextHistoryRoutes.GET("/", chHandler.GetContextHistoryForContextID)
			contextHistoryRoutes.GET("/:hid", chHandler.GetContextHistoryItem)
		}
	}

	return r
}

func (s *ContextServer) Start() {
	mongoClient, err := db.NewMongoClient(s.cfg, s.log)
	if err != nil {
		s.log.Fatalf("Error creating mongo client: %v", err)
	}
	var cRepo = repo.NewContextRepository(s.cfg, s.log, *mongoClient.Client, "context")
	var chRepo = repo.NewContextHistoryRepository(s.cfg, s.log, *mongoClient.Client, "context_history")
	var chSvc = svc.NewContextHistoryService(s.log, chRepo)
	var cSvc = svc.NewContextService(s.log, cRepo, chSvc)

	router := SetupRouter(s.log, cSvc, chSvc)

	serverAddr := fmt.Sprintf(":%d", s.cfg.Server.Port)

	server := &http.Server{
		Addr:           serverAddr,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Fatalf("Error starting server: %v", err)
		}
	}()
	s.log.Infof("Server listening on %s", serverAddr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, os.Kill)
	<-quit
	s.log.Info("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		s.log.Fatalf("Server forced to shutdown (timeout/error): %v", err)
	}
	s.log.Info("Server successfully exited.")
}
