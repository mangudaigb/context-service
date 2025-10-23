package main

import (
	"fmt"

	"github.com/mangudaigb/context-service/internal"
	"github.com/mangudaigb/context-service/pkg"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/consumer"
	"github.com/mangudaigb/dhauli-base/discover"
	"github.com/mangudaigb/dhauli-base/logger"
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

	registry := discover.NewRegistryInfo(cfg, log)
	registry.Register(discover.SERVICE)

	csmr := consumer.NewKafkaConsumer(cfg, log, internal.MsgHandlerFunc)
	defer csmr.Stop()

	server := pkg.NewContextServer(cfg, log)
	server.Start()

}
