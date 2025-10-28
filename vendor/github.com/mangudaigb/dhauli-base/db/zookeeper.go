package db

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
)

type ZkClient struct {
	config    *config.Config
	logger    *logger.Logger
	Conn      *zk.Conn
	EventChan <-chan zk.Event
	ACL       []zk.ACL
}

func NewZkClient(config *config.Config, logger *logger.Logger, sessionTimeOut time.Duration) (*ZkClient, error) {
	conn, eventCh, err := zk.Connect(config.Zookeeper.Servers, sessionTimeOut)
	if err != nil {
		logger.Fatalf("Error while connecting to zookeeper: %v", err)
		return nil, err
	}
	//connected := false
	//timeout := time.After(10 * time.Second)
	//
	//for !connected {
	//	select {
	//	case event := <-eventCh:
	//		logger.Infof("ZooKeeper event: state=%s, type=%s, server=%s", event.State.String(), event.Type.String(), event.Server)
	//		if event.State == zk.StateConnected {
	//			connected = true
	//		}
	//	case <-timeout:
	//		conn.Close()
	//		return nil, fmt.Errorf("timeout while waiting for ZooKeeper connection")
	//	}
	//}
	//
	//logger.Infof("Connected to ZooKeeper servers: %v", config.Zookeeper.Servers)

	return &ZkClient{
		config:    config,
		logger:    logger,
		Conn:      conn,
		EventChan: eventCh,
		ACL:       zk.WorldACL(zk.PermAll),
	}, nil
}

func (zc *ZkClient) Close() {
	if zc.Conn != nil {
		zc.Conn.Close()
	}
}

func (zc *ZkClient) CreateEphemeralNode(path string, data []byte) (bool, error) {
	if zc.Conn == nil {
		return false, errors.New("zookeeper connection is not initialized")
	}
	nodePath, err := zc.Conn.Create(path, data, zk.FlagEphemeral, zc.ACL)
	if err != nil {
		zc.logger.Errorf("Error while creating ephemeral node: %v", err)
		return false, err
	}
	zc.logger.Infof("Ephemeral node created at %s", nodePath)
	return true, nil
}

func (zc *ZkClient) CreateEphemeralSequentialNode(path string, data []byte) (bool, error) {
	if zc.Conn == nil {
		return false, errors.New("zookeeper connection is not initialized")
	}
	flags := zk.FlagEphemeral | zk.FlagSequence
	nodePath, err := zc.Conn.Create(path, data, int32(flags), zc.ACL)
	if err != nil {
		zc.logger.Errorf("failed to create ephemeral sequential node at %s: %w", path, err)
		return false, err
	}
	zc.logger.Infof("Create %d node created at %s", int32(flags), nodePath)
	return true, nil
}

func (zc *ZkClient) GetData(path string) ([]byte, error) {
	if zc.Conn == nil {
		return nil, errors.New("zookeeper connection is not initialized")
	}
	data, _, err := zc.Conn.Get(path)
	if err != nil {
		zc.logger.Errorf("Error while getting data from zookeeper: %v", err)
		return nil, err
	}
	return data, nil
}

func (zc *ZkClient) Delete(path string, version int32) error {
	if zc.Conn == nil {
		return errors.New("zookeeper connection is not initialized")
	}
	err := zc.Conn.Delete(path, version)
	if err != nil {
		zc.logger.Errorf("Error while deleting node: %v", err)
		return err
	}
	zc.logger.Infof("Node deleted at %s", path)
	return nil
}

func (zc *ZkClient) EnsurePath(path string) error {
	if path == "/" {
		return nil // Root always exists
	}

	parts := strings.Split(path, "/")
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath = filepath.Join(currentPath, "/", part)
		exists, _, err := zc.Conn.Exists(currentPath)
		if err != nil {
			return err
		}
		if !exists {
			//_, err := zc.CreateEphemeralNode(currentPath, nil)
			_, err := zc.Conn.Create(currentPath, nil, zk.FlagPersistent, zc.ACL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
