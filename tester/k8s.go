package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/robert-oleynik/k8s-playground/tester/config"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func k8sConnect(conf *config.Config) func() tea.Msg {
	return func() tea.Msg {
		conf, err := clientcmd.BuildConfigFromFlags("", conf.Kubernetes.ConfigPath)
		if err != nil {
			return fmt.Errorf("k8s: build config: %w", err)
		}
		client, err := kubernetes.NewForConfig(conf)
		if err != nil {
			return fmt.Errorf("k8s: new client: %w", err)
		}
		return client
	}
}

type NodeProxy struct {
	Name string
	Port uint16
	cmd  *exec.Cmd
}

func k8sConnectPeers(client *kubernetes.Clientset, conf *config.Config) func() tea.Msg {
	return func() tea.Msg {
		req, err := labels.NewRequirement(
			"raft/cluster",
			selection.Equals,
			[]string{conf.Raft.ClusterName})
		if err != nil {
			return fmt.Errorf("k8s: requirement: %w", err)
		}
		selector := labels.NewSelector()
		selector.Add(*req)
		list, err := client.CoreV1().
			Pods(conf.Kubernetes.Namespace).
			List(context.Background(), v1.ListOptions{
				LabelSelector: selector.String(),
			})
		if err != nil {
			return fmt.Errorf("k8s: list pods: %w", err)
		}
		proxies := make([]NodeProxy, len(list.Items)+1)
		servicePort := uint16(conf.Tester.ProxyPortBegin)
		proxies[0] = NodeProxy{
			Name: "service",
			Port: servicePort,
			cmd: forwardPort(
				conf.Kubernetes.Namespace,
				"service", conf.Raft.ServiceName,
				int(servicePort),
				conf.Raft.DefaultPort),
		}
		for i, pod := range list.Items {
			port := uint16(conf.Tester.ProxyPortBegin) + 1
			proxies[i+1] = NodeProxy{
				Name: pod.Name,
				Port: port,
				cmd: forwardPort(
					conf.Kubernetes.Namespace,
					"pod", pod.Name,
					int(port),
					conf.Raft.DefaultPort),
			}
		}
		// TODO: Wait until ready
		time.Sleep(10 * time.Second)

		return proxies
	}
}

func forwardPort(namespace string, ty string, name string, exposedPort int, containerPort string) *exec.Cmd {
	return exec.Command("kubectl",
		"-n", namespace,
		"port-forward",
		ty+"/"+name,
		fmt.Sprintf("%d:%s", exposedPort, containerPort))
}
