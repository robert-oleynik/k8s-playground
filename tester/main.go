package main

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/robert-oleynik/k8s-playground/tester/config"

	"time"
)

type Config struct {
	Namespace       string
	RaftClusterName string
	ServiceName     string
	ProxyPortsBegin int
	Timeout         time.Duration
}

type Test interface {
	Run() (bool, error)
}

func main() {
	// TODO: Change config path
	cfg, err := config.LoadFromPath(context.Background(), "tests.pkl")
	if err != nil {
		panic(err)
	}

	ui := Init(cfg)
	ui.Register(ErrorTest{})
	ui.Register(FailureTest{})
	ui.Register(PassTest{})

	// TODO: Register Tests
	if _, err := tea.NewProgram(ui).Run(); err != nil {
		panic(err)
	}
}
