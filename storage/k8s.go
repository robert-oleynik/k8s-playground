package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/robert-oleynik/k8s-playground/raft"
	"go.uber.org/zap"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type ServiceConfig struct {
	// Namespace to observe for raft cluster pods.
	Namespace string
	// Name of pod this service is deployed in.
	PodName string
	// Name of service to replicate (label `raft/cluster`)
	Name string
	// Index of the given service. Optional: equals to 2<<32-1 if unset.
	Id uint32

	Events chan<- raft.PeerEvent
}

type Service struct {
	Id   uint32
	Name string
	Host string
	Port string
}

type ServiceDiscoverer struct {
	conf      ServiceConfig
	clientset *kubernetes.Clientset
	selector  labels.Selector

	mtx   *sync.RWMutex
	peers []raft.Peer
}

func StartDiscoverer[T interface{}](srv *raft.Raft[T], conf ServiceConfig) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("k8s config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("k8s client: %w", err)
	}
	filter, err := labels.NewRequirement("raft/cluster", selection.Equals, []string{conf.Name})
	if err != nil {
		return fmt.Errorf("k8s requirement: %w", err)
	}
	selector := labels.NewSelector()
	selector = selector.Add(*filter)

	services := make(map[uint32]*raft.Peer)
	for {
		newServices, err := K8sFetchServices(clientset, selector, conf)
		if err != nil {
			zap.L().Error("k8s fetch services", zap.Error(err))
			time.Sleep(15 * time.Second)
			continue
		}
		for _, service := range newServices {
			if _, ok := services[service.Id]; !ok {
				peer := raft.NewPeer(service.Host, service.Port)
				peer.Id = service.Id
				srv.PeerEvents <- raft.PeerEvent{
					Id:     service.Id,
					Peer:   peer,
					Delete: false,
				}
				services[service.Id] = peer
				continue
			}
			s := services[service.Id]
			if s.Host != service.Host || s.Port != service.Port {
				zap.S().Infow("changed",
					"host", s.Host != service.Host,
					"port", s.Port != service.Port)
				peer := raft.NewPeer(s.Host, s.Port)
				peer.Id = service.Id
				srv.PeerEvents <- raft.PeerEvent{
					Id:     service.Id,
					Peer:   peer,
					Delete: false,
				}
				services[service.Id] = peer
			}
		}
		// TODO: Update services
	outer:
		for id := range services {
			for _, service := range newServices {
				if service.Id == id {
					continue outer
				}
			}
			srv.PeerEvents <- raft.PeerEvent{
				Id:     id,
				Delete: true,
			}
			delete(services, id)
		}

		time.Sleep(15 * time.Second)
	}
}

func K8sFetchServices(
	clientset *kubernetes.Clientset,
	selector labels.Selector,
	conf ServiceConfig,
) ([]Service, error) {
	services := []Service{}
	list, err := clientset.CoreV1().
		Pods(conf.Namespace).
		List(context.Background(), v1.ListOptions{
			LabelSelector: selector.String(),
		})
	if err != nil {
		return services, fmt.Errorf("k8s api: %w", err)
	}
	for _, pod := range list.Items {
		if pod.Name == conf.PodName {
			continue
		} else if pod.Status.PodIP == "" {
			continue
		}
		idS, ok := pod.Labels["apps.kubernetes.io/pod-index"]
		if !ok {
			zap.L().Warn("k8s: pod selected but not indexed",
				zap.String("name", pod.Name),
				zap.String("podIP", pod.Status.PodIP))
			continue
		}
		id, err := strconv.ParseUint(idS, 10, 32)
		if err != nil {
			zap.L().Error("k8s: invalid pod index", zap.Error(err))
			continue
		}
		port, ok := pod.Labels["storage/raft.port"]
		if !ok {
			// TODO: Declare global default port
			port = "5000"
		}
		services = append(services, Service{
			Id:   uint32(id + 1),
			Name: pod.Spec.Hostname,
			Host: pod.Status.PodIP,
			Port: port,
		})
	}
	return services, nil
}
