package config

import (
	"log"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server struct {
		Name string `mapstructure:"name"`
		Port int    `mapstructure:"port"`
	} `mapstructure:"server"`
	Redis struct {
		Host     string        `mapstructure:"host"`
		Port     int           `mapstructure:"port"`
		Timeout  time.Duration `mapstructure:"timeout"`
		Username string        `mapstructure:"username"`
		Password string        `mapstructure:"password"`
		UseTLS   bool          `mapstructure:"useTLS"`
	} `mapstructure:"redis"`
	Zookeeper struct {
		Servers []string `mapstructure:"servers"`
	}
	Kafka struct {
		Brokers     []string `mapstructure:"brokers"`
		GroupId     string   `mapstructure:"groupId"`
		Topic       string   `mapstructure:"topic"`
		RouterTopic string   `mapstructure:"routerTopic"`
		MaxBytes    int      `mapstructure:"maxBytes"`
	} `mapstructure:"kafka"`
	Mongo struct {
		Uri      string `mapstructure:"uri"`
		Host     string `mapstructure:"host"`
		Port     int    `mapstructure:"port"`
		Database string `mapstructure:"database"`
		Username string `mapstructure:"username"`
		Password string `mapstructure:"password"`
	} `mapstructure:"mongo"`
	Logger struct {
		Env              string   `mapstructure:"env"`
		Level            string   `mapstructure:"level"`
		Encoding         string   `mapstructure:"encoding"`
		OutputPaths      []string `mapstructure:"outputPaths"`
		ErrorOutputPaths []string `mapstructure:"errorOutputPaths"`
	} `mapstructure:"logger"`
	Discovery struct {
		Id             string `mapstructure:"id"`
		Fqdn           string `mapstructure:"fqdn,omitempty"`
		Protocol       string `mapstructure:"protocol"`
		TopicName      string `mapstructure:"topic"`
		SessionTimeout int    `mapstructure:"sessionTimeout"`
		Version        string `mapstructure:"version"`
	} `mapstructure:"discovery"`
	Tracing struct {
		Enabled bool   `mapstructure:"enabled"`
		Url     string `mapstructure:"url"`
	} `mapstructure:"tracing"`
}

var config *Config

func loadConfig() (*Config, error) {
	config = &Config{}
	viper.AddConfigPath("configs/")
	viper.SetConfigName("application")
	viper.SetConfigType("yaml")
	viper.SetEnvPrefix("APP")
	viper.AutomaticEnv()

	// Load base config
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Fatalf("Fatal Error: Base config file (application.yaml) not found.")
		} else {
			log.Fatalf("Fatal error reading base config file: %s", err)
		}
	}

	// Profile override
	profile := viper.GetString("PROFILE")
	if profile != "" {
		viper.SetConfigName("application-" + profile)
		if err := viper.MergeInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				log.Fatalf("Fatal Error: Profile config file (application-%s.yaml) not found.", profile)
				return nil, err
			} else {
				log.Fatalf("Fatal error merging profile config file: %s", err)
				return nil, err
			}
		} else {
			log.Printf("Profile config file (application-%s.yaml) loaded.", profile)
		}
	} else {
		log.Printf("No profile config file (application-%s.yaml) loaded.", profile)
	}

	// Unmarshal
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Unable to unmarshal final config into struct: %s", err)
		return nil, err
	}
	return config, nil
}

func GetConfig() (*Config, error) {
	if config == nil {
		return loadConfig()
	}
	return config, nil
}
