package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-errors/errors"
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
}

type Service struct {
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

func NewK8sDiscovererWithContext(conf ServiceConfig) (ServiceDiscoverer, error) {
	discoverer := ServiceDiscoverer{
		mtx:   &sync.RWMutex{},
		peers: []raft.Peer{},
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return discoverer, fmt.Errorf("k8s config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return discoverer, fmt.Errorf("k8s client: %w", err)
	}

	req, err := labels.NewRequirement("raft/cluster", selection.Equals, []string{conf.Name})
	if err != nil {
		return discoverer, fmt.Errorf("k8s requirement: %w", err)
	}
	selector := labels.NewSelector()
	selector = selector.Add(*req)

	discoverer.conf = conf
	discoverer.selector = selector
	discoverer.clientset = clientset
	return discoverer, nil
}

func (discoverer *ServiceDiscoverer) ListServicesWithContext(ctx context.Context) ([]Service, error) {
	services := []Service{}
	list, err := discoverer.clientset.CoreV1().
		Pods(discoverer.conf.Namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: discoverer.selector.String(),
		})
	if err != nil {
		return services, fmt.Errorf("k8s api: %w", err)
	}
	for _, pod := range list.Items {
		if pod.Name == discoverer.conf.Name {
			continue
		} else if pod.Status.PodIP == "" {
			continue
		}
		port, ok := pod.Annotations["storage/raft.port"]
		if !ok {
			// TODO: Declare global default port
			port = "5000"
		}
		services = append(services, Service{
			Host: pod.Status.PodIP,
			Port: port,
		})
	}
	return services, nil
}

func (discoverer *ServiceDiscoverer) Discover() error {
	services, err := discoverer.ListServicesWithContext(context.Background())
	if err != nil {
		return fmt.Errorf("%s", err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(services))
	peers := make([]*raft.Peer, len(services))
	for i, service := range services {
		go func() {
			peers[i] = nil
			defer wg.Done()
			peer, err := raft.NewPeer(service.Host, service.Port)
			if err != nil {
				zap.L().Error("failed to create peer", zap.Error(err))
				return
			}
			if err := peer.FetchInfo(); err != nil {
				zap.L().Error("failed to fetch info", zap.Error(err))
			} else {
				peers[i] = &peer
			}
		}()
	}
	wg.Wait()
	discoverer.mtx.Lock()
	defer discoverer.mtx.Unlock()
	discoverer.peers = make([]raft.Peer, len(peers))
	for i, peer := range peers {
		if peer == nil {
			return errors.New("failed to discover all peers")
		}
		discoverer.peers[i] = *peer
	}
	return nil
}

func (discoverer *ServiceDiscoverer) Peers() []raft.Peer {
	discoverer.mtx.RLock()
	discoverer.mtx.RUnlock()
	peers := make([]raft.Peer, len(discoverer.peers))
	copy(peers, discoverer.peers)
	return peers
}
