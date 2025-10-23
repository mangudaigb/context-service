package pkg

import (
	"fmt"
	"net/http"

	db2 "github.com/mangudaigb/context-service/db"
	"github.com/mangudaigb/context-service/internal"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/consumer"
	"github.com/mangudaigb/dhauli-base/db"
	"github.com/mangudaigb/dhauli-base/logger"
)

type HttpServer struct {
	*consumer.KafkaConsumer
	zkClient *db.ZkClient
}

type ContextServer struct {
	log *logger.Logger
	cfg *config.Config
}

func NewContextServer(cfg *config.Config, log *logger.Logger) *ContextServer {
	return &ContextServer{
		log: log,
		cfg: cfg,
	}
}

func (s *ContextServer) Start() {
	var repo = db2.NewContextRepository(s.cfg, s.log, "context")

	router := internal.SetupRouter(s.log, repo)

	serverAddr := fmt.Sprintf(":%d", s.cfg.Server.Port)
	if err := http.ListenAndServe(serverAddr, router); err != nil {
		s.log.Fatalf("Error starting server: %v", err)
	}
}
