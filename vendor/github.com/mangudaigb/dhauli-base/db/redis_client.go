package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	log    *logger.Logger
	client *redis.ClusterClient
}

func NewRedisClient(cfg *config.Config, log *logger.Logger) (*RedisClient, error) {
	opts := &redis.ClusterOptions{
		Addrs:    strings.Split(cfg.Redis.Host, ","),
		Username: cfg.Redis.Username,
		Password: cfg.Redis.Password,
	}

	if cfg.Redis.UseTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: false,
		}
	}
	client := redis.NewClusterClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %v", err)
	}

	return &RedisClient{
		log:    log,
		client: client,
	}, nil
}
