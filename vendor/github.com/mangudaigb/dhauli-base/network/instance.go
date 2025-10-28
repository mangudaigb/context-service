package network

import (
	"errors"
	"net"

	"github.com/mangudaigb/dhauli-base/config"
	"github.com/mangudaigb/dhauli-base/logger"
)

type InstanceAddr struct {
	Name       string `json:"name,omitempty"`
	Ip         string `json:"ip"`
	Port       int    `json:"port"`
	DataCenter string `json:"datacenter,omitempty"`
	Region     string `json:"region,omitempty"`
	Zone       string `json:"zone,omitempty"`
	Rack       string `json:"rack,omitempty"`
	Cluster    string `json:"cluster,omitempty"`
}

func GetIp(log *logger.Logger) (string, error) {
	interfaces, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, i := range interfaces {
		if ipnet, ok := i.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			log.Infof("ipnet: %v", ipnet)
			if ipnet.IP.To4() != nil {
				ip := ipnet.IP.To4()
				if ip.IsPrivate() {
					return ipnet.IP.String(), nil
				}
			}
		}
	}
	return "", errors.New("could not find a valid private IP address")
}

func GetInstanceAddr(cfg *config.Config, log *logger.Logger) (*InstanceAddr, error) {
	ip, err := GetIp(log)
	if err != nil {
		log.Error("Error: %v", err)
		return nil, err
	}
	port := cfg.Server.Port
	log.Infof("Local Private Ip Address: %s Port: %d", ip, port)
	return &InstanceAddr{
		Name: "none",
		Ip:   ip,
		Port: port,
	}, nil
}
