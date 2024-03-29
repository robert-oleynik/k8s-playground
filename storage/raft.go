package main

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/robert-oleynik/k8s-playground/raft"
)

type State struct {
	guard *sync.RWMutex
	data  map[uuid.UUID]string
}

type RaftCommand struct {
	Id     uuid.UUID
	Data   string
	Delete bool
}

func LaunchRaftWithContext(port uint16, serviceConf ServiceConfig, ctx context.Context) error {
	conf := raft.DefaultConfig()

	k8sDiscoverer, err := NewK8sDiscovererWithContext(serviceConf)
	if err != nil {
		return fmt.Errorf("k8s: %w", err)
	}

	r := raft.NewWithConfig[RaftCommand](conf)
	r.Discoverer = &k8sDiscoverer
	state := &State{
		guard: &sync.RWMutex{},
		data:  make(map[uuid.UUID]string),
	}
	go func() {
		for {
			select {
			case cmd := <-r.Apply:
				state.Apply(cmd)
			case <-ctx.Done():
				return
			}
		}
	}()
	r.Log = raft.NewMemoryLog[RaftCommand]()

	listener, err := net.Listen("tcp", fmt.Sprintf("[::]:%d", port))
	if err != nil {
		return fmt.Errorf("listener: %w", err)
	} else if err := r.Serve(listener); err != nil {
		return fmt.Errorf("raft: %w", err)
	}
	return nil
}

func (state *State) Apply(cmd RaftCommand) error {
	state.guard.Lock()
	defer state.guard.RUnlock()
	if cmd.Delete {
		delete(state.data, cmd.Id)
	} else {
		state.data[cmd.Id] = cmd.Data
	}
	return nil
}
