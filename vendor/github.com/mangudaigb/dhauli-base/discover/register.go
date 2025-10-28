package discover

import (
	"encoding/json"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/db"

	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/mangudaigb/dhauli-base/network"
)

type InstanceInfo struct {
	Id        string `json:"id"`
	Fqdn      string `json:"fqdn"`
	Protocol  string `json:"protocol"`
	Ip        string `json:"ip"`
	Port      int    `json:"port"`
	TopicName string `json:"topicName"`
	Version   string `json:"version"`
}

type AppType int

const (
	SERVICE AppType = iota
	AGENT
	MCP
	LAMBDA
)

var basePath = "/dhauli"

func NewInstanceInfo(config *config.Config, logger *logger.Logger) (*InstanceInfo, error) {
	ip, err := network.GetIp(logger)
	if err != nil {
		logger.Errorf("Error while getting ip: %v", err)
		return nil, err
	}

	var id string
	if config.Discovery.Id != "" {
		id = config.Discovery.Id
	} else {
		id = uuid.New().String()
	}

	return &InstanceInfo{
		Id:        id,
		Fqdn:      "not-yet-implemented",
		Protocol:  config.Discovery.Protocol, // "com.apple.dhauli.events.json.v1"
		Port:      config.Server.Port,
		Ip:        ip,
		TopicName: config.Discovery.TopicName,
		Version:   config.Discovery.Version,
	}, nil
}

type Registry struct {
	config *config.Config
	log    *logger.Logger
	client *db.ZkClient
}

func NewRegistryInfo(config *config.Config, logger *logger.Logger) *Registry {

	return &Registry{
		config: config,
		log:    logger,
	}
}

func (r *Registry) Register(appType AppType) *InstanceInfo {
	registrationInfo, err := r.registerInstance(appType)
	if err != nil {
		r.log.Fatalf("Error while registering instance: %v", err)
	}
	r.log.Info("Instance registered successfully.")

	//go r.monitorSession(appType)

	return registrationInfo
}

func (r *Registry) registerInstance(appType AppType) (*InstanceInfo, error) {
	sessionTimeout := time.Minute * time.Duration(r.config.Discovery.SessionTimeout)
	client, err := db.NewZkClient(r.config, r.log, sessionTimeout)

	if err != nil {
		r.log.Fatalf("Error while creating zookeeper client: %v", err)
	}
	//defer client.Close()

	switch appType {
	case SERVICE:
		basePath = "/dhauli/services"
	case AGENT:
		basePath = "/dhauli/agents"
	case MCP:
		basePath = "/dhauli/mcp"
	case LAMBDA:
		basePath = "/dhauli/lambda"
	default:
		panic("unknown app type")
	}
	path := basePath + "/" + r.config.Server.Name + "/instances"
	if err := client.EnsurePath(path); err != nil {
		panic("error while creating the application path and not instance path in zookeeper.")
	}

	instanceInfo, err := NewInstanceInfo(r.config, r.log)
	if err != nil {
		panic("could not create instance info")
	}
	instanceInfoBytes, err := json.Marshal(instanceInfo)
	if err != nil {
		r.log.Fatalf("could not marshal instance info %v because of error %v", instanceInfo, err)
	}

	path = path + "/" + instanceInfo.Id
	status, err := client.CreateEphemeralNode(path, instanceInfoBytes)
	if err != nil {
		r.log.Fatalf("could not create instance path %v", err)
		return nil, err
	}
	r.log.Infof("Instance registeration status:  %t", status)

	return instanceInfo, nil
}

func (r *Registry) monitorSession(appType AppType) {
	for event := range r.client.EventChan {
		r.log.Infof("ZooKeeper Event Received: Type=%s, State=%s, Server=%s", event.Type.String(), event.State.String(), event.Server)

		switch event.State {
		case zk.StateConnected:
			r.log.Info("Initial connection established. Attempting registration...")
			if _, err := r.registerInstance(appType); err != nil {
				r.log.Fatalf("Error while retrying to register instance: %v", err)
			}
		case zk.StateConnecting:
			r.log.Info("Attempting to connect or re-establish session...")
		case zk.StateHasSession:
			r.log.Info("Session successfully re-established/recovered. Checking registration...")
			if _, err := r.registerInstance(appType); err != nil {
				r.log.Fatalf("Error while registering instance: %v after recovery...", err)
			}
		case zk.StateExpired:
			r.log.Info("Session Expired! Ephemeral node is deleted. Must establish a new connection and re-register.")
		case zk.StateDisconnected:
			r.log.Info("Connection lost. Waiting for automatic reconnection...")
		}
	}
}

func (r *Registry) Close() {
	r.client.Close()
	r.log.Info("Zookeeper connection closed and ephemeral nodes deleted")
}
